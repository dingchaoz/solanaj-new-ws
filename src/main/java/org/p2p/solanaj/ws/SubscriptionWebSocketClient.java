package org.p2p.solanaj.ws;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;
import org.p2p.solanaj.rpc.types.config.Commitment;
import org.p2p.solanaj.rpc.types.RpcRequest;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.Types;

import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import org.p2p.solanaj.ws.listeners.NotificationEventListener;
import org.p2p.solanaj.rpc.types.RpcNotificationResult;
import org.p2p.solanaj.rpc.types.RpcResponse;

/**
 * SubscriptionWebSocketClient using Spring WebSocket with Reactive Stack (Project Reactor)
 * This implementation provides better scalability and non-blocking operations compared to
 * the traditional WebSocket implementation.
 */
public class SubscriptionWebSocketClient {

    private static final Logger LOGGER = Logger.getLogger(SubscriptionWebSocketClient.class.getName());
    
    // Spring WebSocket client using Reactor Netty
    private final ReactorNettyWebSocketClient webSocketClient;
    
    // Reactive components for handling messages
    private final Sinks.Many<String> messageSink;
    private final Flux<String> messageFlux;
    private WebSocketSession session;
    
    // Keep existing fields for subscription management
    private final Map<String, SubscriptionParams> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, Long> subscriptionIds = new ConcurrentHashMap<>();
    private final Map<Long, NotificationEventListener> subscriptionListeners = new ConcurrentHashMap<>();
    
    // Reactive scheduler for handling background tasks
    private final reactor.core.scheduler.Scheduler scheduler = Schedulers.boundedElastic();

    // Add these field declarations at the class level
    private final Moshi moshi = new Moshi.Builder().build();
    private final Lock subscriptionLock = new ReentrantLock();
    private final Map<String, SubscriptionParams> activeSubscriptions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private static final int HEARTBEAT_INTERVAL = 30; // seconds
    private final Lock listenerLock = new ReentrantLock();
    private long reconnectDelay = 1000; // Initial delay in milliseconds
    private static final long MAX_RECONNECT_DELAY = 60000; // Maximum delay in milliseconds
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    /**
     * Inner class to hold subscription parameters.
     */
    private static class SubscriptionParams {
        final RpcRequest request;
        final NotificationEventListener listener;

        /**
         * Constructs a SubscriptionParams object.
         *
         * @param request The RPC request for the subscription
         * @param listener The listener for notification events
         */
        SubscriptionParams(RpcRequest request, NotificationEventListener listener) {
            this.request = request;
            this.listener = listener;
        }
    }

    /**
     * Creates a SubscriptionWebSocketClient instance with the exact path provided.
     *
     * @param endpoint The WebSocket endpoint URL
     * @return A new SubscriptionWebSocketClient instance
     */
    public static SubscriptionWebSocketClient getExactPathInstance(String endpoint) {
        try {
            URI serverURI = new URI(endpoint);
            SubscriptionWebSocketClient instance = new SubscriptionWebSocketClient(serverURI);
            if (!instance.isOpen()) {
                instance.connect(serverURI);
            }
            return instance;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid endpoint URI", e);
        }
    }

    /**
     * Creates a SubscriptionWebSocketClient instance with a modified URI based on the provided endpoint.
     *
     * @param endpoint The endpoint URL
     * @return A new SubscriptionWebSocketClient instance
     */
    public static SubscriptionWebSocketClient getInstance(String endpoint) {
        try {
            URI endpointURI = new URI(endpoint);
            String scheme = "https".equals(endpointURI.getScheme()) ? "wss" : "ws";
            URI serverURI = new URI(scheme + "://" + endpointURI.getHost());
            SubscriptionWebSocketClient instance = new SubscriptionWebSocketClient(serverURI);
            instance.connect(serverURI);
            return instance;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid endpoint URI", e);
        }
    }

    /**
     * Constructs a SubscriptionWebSocketClient with the given server URI.
     *
     * @param serverURI The URI of the WebSocket server
     */
    public SubscriptionWebSocketClient(URI serverURI) {
        // Initialize Spring WebSocket client
        this.webSocketClient = new ReactorNettyWebSocketClient();
        
        // Create a many-to-many broadcast sink for message handling
        this.messageSink = Sinks.many().multicast().onBackpressureBuffer();
        this.messageFlux = messageSink.asFlux()
                .publishOn(scheduler)
                .share();
        
        // Connect to WebSocket server
        connect(serverURI);
    }

    /**
     * Establishes WebSocket connection using Spring's Reactive WebSocket client
     */
    private void connect(URI serverURI) {
        webSocketClient.execute(serverURI, this::handleWebSocketSession)
                .doOnSuccess(unused -> connectLatch.countDown())
                .doOnError(error -> {
                    LOGGER.severe("WebSocket connection error: " + error.getMessage());
                    connectLatch.countDown();
                })
                .subscribe();
    }

    /**
     * Handles the WebSocket session using Reactive streams
     * This is where Spring WebSocket and Project Reactor integrate
     */
    private Mono<Void> handleWebSocketSession(WebSocketSession session) {
        this.session = session;
        startHeartbeat();
        
        // Handle incoming messages
        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(this::handleIncomingMessage)
                .then();

        // Handle outgoing messages
        Mono<Void> send = messageFlux
                .map(session::textMessage)
                .as(session::send);

        return Mono.zip(receive, send).then();
    }

    /**
     * Handles incoming messages using Reactive approach
     */
    private void handleIncomingMessage(String message) {
        try {
            // Process message using existing logic but in a reactive way
            Mono.fromCallable(() -> {
                JsonAdapter<RpcResponse<Long>> resultAdapter = moshi.adapter(
                        Types.newParameterizedType(RpcResponse.class, Long.class));
                RpcResponse<Long> rpcResult = resultAdapter.fromJson(message);
                
                if (rpcResult != null && rpcResult.getError() != null) {
                    throw new IllegalStateException(rpcResult.getError().toString());
                }
                
                if (rpcResult != null && rpcResult.getId() != null) {
                    handleSubscriptionResponse(rpcResult);
                } else {
                    handleNotification(message);
                }
                return true;
            })
            .subscribeOn(scheduler)
            .subscribe(
                success -> {},
                error -> LOGGER.severe("Error processing message: " + error.getMessage())
            );
        } catch (Exception ex) {
            LOGGER.severe("Error processing message: " + ex.getMessage());
        }
    }

    /**
     * Sends a message through the WebSocket connection using Reactive approach
     */
    private void sendMessage(String message) {
        messageSink.tryEmitNext(message)
                .orThrow(); // Handle emission failure
    }

    /**
     * Subscribes to account updates for the given key with specified commitment level and encoding.
     *
     * @param key The account key to subscribe to
     * @param listener The listener to handle notifications
     * @param commitment The commitment level for the subscription
     * @param encoding The encoding format for Account data
     */
    public void accountSubscribe(String key, NotificationEventListener listener, Commitment commitment, String encoding) {
        List<Object> params = new ArrayList<>();
        params.add(key);
        params.add(Map.of("encoding", encoding, "commitment", commitment.getValue()));

        RpcRequest rpcRequest = new RpcRequest("accountSubscribe", params);
        
        // Add subscription using Reactive approach
        Mono.fromRunnable(() -> addSubscription(rpcRequest, listener))
            .subscribeOn(scheduler)
            .subscribe();
    }

    // Overload methods to maintain backwards compatibility
    public void accountSubscribe(String key, NotificationEventListener listener, Commitment commitment) {
        accountSubscribe(key, listener, commitment, "jsonParsed");
    }

    public void accountSubscribe(String key, NotificationEventListener listener) {
        accountSubscribe(key, listener, Commitment.FINALIZED, "jsonParsed");
    }

    /**
     * Subscribes to signature updates for the given signature.
     *
     * @param signature The signature to subscribe to
     * @param listener The listener to handle notifications
     */
    public void signatureSubscribe(String signature, NotificationEventListener listener) {
        List<Object> params = new ArrayList<>();
        params.add(signature);

        RpcRequest rpcRequest = new RpcRequest("signatureSubscribe", params);
        addSubscription(rpcRequest, listener);
    }

    /**
     * Subscribes to log updates for the given mention.
     *
     * @param mention The mention to subscribe to
     * @param listener The listener to handle notifications
     */
    public void logsSubscribe(String mention, NotificationEventListener listener) {
        logsSubscribe(List.of(mention), listener);
    }

    /**
     * Subscribes to log updates for the given mentions.
     *
     * @param mentions The mentions to subscribe to
     * @param listener The listener to handle notifications
     */
    public void logsSubscribe(List<String> mentions, NotificationEventListener listener) {
        List<Object> params = new ArrayList<>();
        params.add(Map.of("mentions", mentions));
        params.add(Map.of("commitment", "finalized"));

        RpcRequest rpcRequest = new RpcRequest("logsSubscribe", params);
        addSubscription(rpcRequest, listener);
    }

    /**
     * Subscribes to block updates.
     *
     * @param listener The listener to handle notifications
     * @param commitment The commitment level for the subscription
     * @param encoding The encoding format for block data
     */
    public void blockSubscribe(NotificationEventListener listener, Commitment commitment, String encoding) {
        List<Object> params = new ArrayList<>();
        params.add(Map.of("encoding", encoding, "commitment", commitment.getValue()));

        RpcRequest rpcRequest = new RpcRequest("blockSubscribe", params);
        addSubscription(rpcRequest, listener);
    }

    public void blockSubscribe(NotificationEventListener listener, Commitment commitment) {
        blockSubscribe(listener, commitment, "json");
    }

    public void blockSubscribe(NotificationEventListener listener) {
        blockSubscribe(listener, Commitment.FINALIZED, "json");
    }

    /**
     * Unsubscribes from block updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void blockUnsubscribe(String subscriptionId) {
        unsubscribe("blockUnsubscribe", subscriptionId);
    }

    /**
     * Subscribes to program updates.
     *
     * @param programId The program ID to subscribe to
     * @param listener The listener to handle notifications
     * @param commitment The commitment level for the subscription
     * @param encoding The encoding format for program data
     */
    public void programSubscribe(String programId, NotificationEventListener listener, Commitment commitment, String encoding) {
        List<Object> params = new ArrayList<>();
        params.add(programId);
        params.add(Map.of("encoding", encoding, "commitment", commitment.getValue()));

        RpcRequest rpcRequest = new RpcRequest("programSubscribe", params);
        addSubscription(rpcRequest, listener);
    }

    public void programSubscribe(String programId, NotificationEventListener listener, Commitment commitment) {
        programSubscribe(programId, listener, commitment, "base64");
    }

    public void programSubscribe(String programId, NotificationEventListener listener) {
        programSubscribe(programId, listener, Commitment.FINALIZED, "base64");
    }

    /**
     * Unsubscribes from program updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void programUnsubscribe(String subscriptionId) {
        unsubscribe("programUnsubscribe", subscriptionId);
    }

    /**
     * Subscribes to root updates.
     *
     * @param listener The listener to handle notifications
     */
    public void rootSubscribe(NotificationEventListener listener) {
        RpcRequest rpcRequest = new RpcRequest("rootSubscribe", new ArrayList<>());
        addSubscription(rpcRequest, listener);
    }

    /**
     * Unsubscribes from root updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void rootUnsubscribe(String subscriptionId) {
        unsubscribe("rootUnsubscribe", subscriptionId);
    }

    /**
     * Subscribes to slot updates.
     *
     * @param listener The listener to handle notifications
     */
    public void slotSubscribe(NotificationEventListener listener) {
        RpcRequest rpcRequest = new RpcRequest("slotSubscribe", new ArrayList<>());
        addSubscription(rpcRequest, listener);
    }

    /**
     * Unsubscribes from slot updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void slotUnsubscribe(String subscriptionId) {
        unsubscribe("slotUnsubscribe", subscriptionId);
    }

    /**
     * Subscribes to slots updates.
     *
     * @param listener The listener to handle notifications
     */
    public void slotsUpdatesSubscribe(NotificationEventListener listener) {
        RpcRequest rpcRequest = new RpcRequest("slotsUpdatesSubscribe", new ArrayList<>());
        addSubscription(rpcRequest, listener);
    }

    /**
     * Unsubscribes from slots updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void slotsUpdatesUnsubscribe(String subscriptionId) {
        unsubscribe("slotsUpdatesUnsubscribe", subscriptionId);
    }

    /**
     * Subscribes to vote updates.
     *
     * @param listener The listener to handle notifications
     */
    public void voteSubscribe(NotificationEventListener listener) {
        RpcRequest rpcRequest = new RpcRequest("voteSubscribe", new ArrayList<>());
        addSubscription(rpcRequest, listener);
    }

    /**
     * Unsubscribes from vote updates.
     *
     * @param subscriptionId The ID of the subscription to cancel
     */
    public void voteUnsubscribe(String subscriptionId) {
        unsubscribe("voteUnsubscribe", subscriptionId);
    }

    /**
     * Generic method to handle unsubscribe requests.
     *
     * @param method The unsubscribe method name
     * @param subscriptionId The ID of the subscription to cancel
     */
    private void unsubscribe(String method, String subscriptionId) {
        List<Object> params = new ArrayList<>();
        params.add(Long.parseLong(subscriptionId));
        RpcRequest unsubRequest = new RpcRequest(method, params);
        JsonAdapter<RpcRequest> rpcRequestJsonAdapter = moshi.adapter(RpcRequest.class);
        send(rpcRequestJsonAdapter.toJson(unsubRequest));

        subscriptionLock.lock();
        try {
            activeSubscriptions.remove(subscriptionId);
            subscriptionListeners.remove(Long.parseLong(subscriptionId));
        } finally {
            subscriptionLock.unlock();
        }
        LOGGER.info("Unsubscribed from " + method + " with ID: " + subscriptionId);
    }

    /**
     * Adds a subscription to the client.
     *
     * @param rpcRequest The RPC request for the subscription
     * @param listener The listener for notification events
     */
    public void addSubscription(RpcRequest rpcRequest, NotificationEventListener listener) {
        String subscriptionId = rpcRequest.getId();
        subscriptionLock.lock();
        try {
            activeSubscriptions.put(subscriptionId, new SubscriptionParams(rpcRequest, listener));
            subscriptions.put(subscriptionId, new SubscriptionParams(rpcRequest, listener));
            subscriptionIds.put(subscriptionId, 0L);
        } finally {
            subscriptionLock.unlock();
        }
        updateSubscriptions();
    }

    /**
     * Handles subscription responses.
     *
     * @param rpcResult The RPC response
     */
    private void handleSubscriptionResponse(RpcResponse<Long> rpcResult) {
        String rpcResultId = rpcResult.getId();
        if (subscriptionIds.containsKey(rpcResultId)) {
            subscriptionIds.put(rpcResultId, rpcResult.getResult());
            SubscriptionParams params = subscriptions.get(rpcResultId);
            if (params != null) {
                subscriptionListeners.put(rpcResult.getResult(), params.listener);
                subscriptions.remove(rpcResultId);
                // Update the activeSubscriptions map with the new subscription ID
                activeSubscriptions.put(String.valueOf(rpcResult.getResult()), params);
                activeSubscriptions.remove(rpcResultId);
            }
        }
    }

    /**
     * Handles notification messages.
     *
     * @param message The notification message
     * @throws Exception If an error occurs while processing the notification
     */
    private void handleNotification(String message) throws Exception {
        JsonAdapter<RpcNotificationResult> notificationResultAdapter = moshi.adapter(RpcNotificationResult.class);
        RpcNotificationResult result = notificationResultAdapter.fromJson(message);
        if (result != null) {
            Long subscriptionId = result.getParams().getSubscription();
            listenerLock.lock();
            try {
                NotificationEventListener listener = subscriptionListeners.get(subscriptionId);
                if (listener != null) {
                    Map<String, Object> value = (Map<String, Object>) result.getParams().getResult().getValue();
                    switch (result.getMethod()) {
                        case "signatureNotification":
                            listener.onNotificationEvent(new SignatureNotification(value.get("err")));
                            break;
                        case "accountNotification":
                        case "logsNotification":
                        case "blockNotification":
                        case "programNotification":
                        case "rootNotification":
                        case "slotNotification":
                        case "slotsUpdatesNotification":
                        case "voteNotification":
                            listener.onNotificationEvent(value);
                            break;
                        default:
                            LOGGER.warning("Unknown notification method: " + result.getMethod());
                    }
                } else {
                    LOGGER.warning("No listener found for subscription ID: " + subscriptionId);
                }
            } finally {
                listenerLock.unlock();
            }
        } else {
            LOGGER.warning("Received null notification result");
        }
    }

    /**
     * Handles WebSocket connection closure.
     *
     * @param code The status code indicating why the connection was closed
     * @param reason A human-readable explanation for the closure
     * @param remote Whether the closure was initiated by the remote endpoint
     */
    public void onError(Exception ex) {
        LOGGER.log(Level.SEVERE, "WebSocket error occurred", ex);
        if (ex instanceof org.java_websocket.exceptions.WebsocketNotConnectedException) {
            LOGGER.severe("WebSocket is not connected. Attempting to reconnect...");
            reconnect();
        } else if (ex instanceof org.java_websocket.exceptions.IncompleteHandshakeException) {
            LOGGER.severe("Incomplete handshake. Check your connection parameters.");
        } else if (ex instanceof java.net.SocketTimeoutException) {
            LOGGER.severe("Connection timed out. Check network stability and server responsiveness.");
        } else {
            LOGGER.severe("Unexpected error: " + ex.getMessage());
        }
    }

    /**
     * Attempts to reconnect to the WebSocket server.
     */
    public void reconnect() {
        LOGGER.info("Attempting to reconnect...");
        try {
            final boolean reconnectBlocking = reconnectBlocking();
            if(reconnectBlocking){
                resubscribeAll();
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Reconnection interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the heartbeat mechanism to keep the connection alive.
     */
    private void startHeartbeat() {
        executor.scheduleAtFixedRate(this::sendPing, (long)HEARTBEAT_INTERVAL, (long)HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * Stops the heartbeat mechanism.
     */
    private void stopHeartbeat() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Updates all active subscriptions.
     */
    private void updateSubscriptions() {
        if (isOpen()) {
            JsonAdapter<RpcRequest> rpcRequestJsonAdapter = moshi.adapter(RpcRequest.class);
            for (SubscriptionParams sub : subscriptions.values()) {
                send(rpcRequestJsonAdapter.toJson(sub.request));
            }
            for (Map.Entry<String, Long> entry : subscriptionIds.entrySet()) {
                if (entry.getValue() != 0L) {
                    SubscriptionParams params = subscriptions.get(entry.getKey());
                    if (params != null) {
                        send(rpcRequestJsonAdapter.toJson(params.request));
                    }
                }
            }
        }
    }

    /**
     * Schedules a reconnection attempt with exponential backoff.
     */
    private void scheduleReconnect() {
        executor.schedule(() -> {
            reconnect();
            reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
        }, reconnectDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits for the WebSocket connection to be established.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if the connection was successfully established, false if the timeout was reached
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean waitForConnection(long timeout, TimeUnit unit) throws InterruptedException {
        boolean connected = connectLatch.await(timeout, unit);
        return connected && isOpen();
    }

    private void resubscribeAll() {
        LOGGER.info("Resubscribing to all active subscriptions");
        cleanSubscriptions();
        final Map<String, SubscriptionParams> activeSubscriptionsResubscribe = new HashMap<>();
        for (Map.Entry<String, SubscriptionParams> entry : activeSubscriptions.entrySet()) {
            SubscriptionParams paramsOld = entry.getValue();
            final RpcRequest rpcRequest = paramsOld.request;
            final NotificationEventListener notificationEventListener = paramsOld.listener;
            final RpcRequest request = new RpcRequest(rpcRequest.getMethod(), rpcRequest.getParams());
            final String subscriptionId = request.getId();
            final SubscriptionParams params = new SubscriptionParams(
                    request,
                    notificationEventListener);
            subscriptions.put(subscriptionId, params);
            subscriptionIds.put(subscriptionId, 0L);
            activeSubscriptionsResubscribe.put(subscriptionId, params);
        }
        activeSubscriptions.clear();
        activeSubscriptions.putAll(activeSubscriptionsResubscribe);
        updateSubscriptions();
    }

    public void cleanSubscriptions(){
        subscriptions.clear();
        subscriptionIds.clear();
        subscriptionListeners.clear();
    }

    public void unsubscribe(String subscriptionId) {
        SubscriptionParams params = activeSubscriptions.remove(subscriptionId);
        if (params != null) {
            // Send an unsubscribe request to the server
            List<Object> unsubParams = new ArrayList<>();
            unsubParams.add(Long.parseLong(subscriptionId));
            RpcRequest unsubRequest = new RpcRequest(getUnsubscribeMethod(params.request.getMethod()), unsubParams);
            JsonAdapter<RpcRequest> rpcRequestJsonAdapter = moshi.adapter(RpcRequest.class);
            send(rpcRequestJsonAdapter.toJson(unsubRequest));

            // Remove the subscription from subscriptionListeners
            subscriptionListeners.remove(Long.parseLong(subscriptionId));
            LOGGER.info("Unsubscribed from subscription: " + subscriptionId);
        } else {
            LOGGER.warning("Attempted to unsubscribe from non-existent subscription: " + subscriptionId);
        }
    }

    private String getUnsubscribeMethod(String subscribeMethod) {
        switch (subscribeMethod) {
            case "accountSubscribe":
                return "accountUnsubscribe";
            case "logsSubscribe":
                return "logsUnsubscribe";
            case "signatureSubscribe":
                return "signatureUnsubscribe";
            case "blockSubscribe":
                return "blockUnsubscribe";
            case "programSubscribe":
                return "programUnsubscribe";
            case "rootSubscribe":
                return "rootUnsubscribe";
            case "slotSubscribe":
                return "slotUnsubscribe";
            case "slotsUpdatesSubscribe":
                return "slotsUpdatesUnsubscribe";
            case "voteSubscribe":
                return "voteUnsubscribe";
            // Add more cases for other subscription types as needed
            default:
                throw new IllegalArgumentException("Unknown subscribe method: " + subscribeMethod);
        }
    }

    /**
     * Gets the subscription ID for a given account.
     *
     * @param account The account to get the subscription ID for
     * @return The subscription ID, or null if not found
     */
    public String getSubscriptionId(String account) {
        for (Map.Entry<String, SubscriptionParams> entry : activeSubscriptions.entrySet()) {
            if (entry.getValue().request.getParams().get(0).equals(account)) {
                return entry.getKey();
            }
        }
        return null;
    }

    // Add this method to the SubscriptionWebSocketClient class
    public boolean isOpen() {
        return session != null && session.isOpen();
    }

    // Add this method for sending messages
    public void send(String message) {
        sendMessage(message);
    }

    private boolean reconnectBlocking() throws InterruptedException {
        connect(session.getHandshakeInfo().getUri());
        return isOpen();
    }

    private void sendPing() {
        if (isOpen()) {
            send("{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}");
        }
    }

    public Flux<String> getMessageFlux() {
        return messageFlux;
    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }

}