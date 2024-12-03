package org.p2p.solanaj.ws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import reactor.test.StepVerifier;

class SubscriptionWebSocketClientTest {

    private static final String DEVNET_WS_URL = "wss://api.devnet.solana.com";
    private SubscriptionWebSocketClient client;
    private CountDownLatch connectionLatch;

    @BeforeEach
    void setUp() throws Exception {
        connectionLatch = new CountDownLatch(1);
        client = SubscriptionWebSocketClient.getExactPathInstance(DEVNET_WS_URL);
        
        // Increase timeout to 30 seconds for test environment
        if (!client.waitForConnection(30, TimeUnit.SECONDS)) {
            fail("Failed to establish WebSocket connection within timeout period");
        }
        
        // Additional verification
        assertTrue(client.isOpen(), "WebSocket connection should be open");
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            // Close all subscriptions and cleanup
            client.cleanSubscriptions();
        }
    }

    @Test
    void testConnectionEstablished() {
        assertTrue(client.isOpen(), "WebSocket should be open");
    }

    @Test
    void testSendAndReceiveMessage() throws Exception {
        CountDownLatch messageLatch = new CountDownLatch(1);
        final String[] receivedMessage = new String[1];

        client = SubscriptionWebSocketClient.getExactPathInstance(DEVNET_WS_URL);
        assertTrue(client.waitForConnection(10, TimeUnit.SECONDS), "Connection timed out");

        // Send a health check message
        String testMessage = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getHealth\"}";
        client.send(testMessage);

        // Wait for response using reactive streams
        StepVerifier.create(client.getMessageFlux()
                .next())
                .assertNext(message -> {
                    assertNotNull(message, "Received message should not be null");
                    assertTrue(message.contains("result") || message.contains("error"),
                            "Received message should contain 'result' or 'error'");
                })
                .verifyComplete();
    }

    @Test
    void testConnectionCloseAndReconnect() throws Exception {
        // Force close the connection
        client.close();
        assertFalse(client.isOpen(), "WebSocket should be closed");

        // Create new connection
        client = SubscriptionWebSocketClient.getExactPathInstance(DEVNET_WS_URL);
        assertTrue(client.waitForConnection(10, TimeUnit.SECONDS), "Reconnection timed out");
        assertTrue(client.isOpen(), "WebSocket should be open after reconnection");
    }
}
