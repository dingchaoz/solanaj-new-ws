package org.p2p.solanaj.ws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import reactor.test.StepVerifier;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import reactor.core.publisher.Mono;
import java.util.HashMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.HandlerMapping;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SubscriptionWebSocketClientTestLocalServer {

    @LocalServerPort
    private int port;

    private SubscriptionWebSocketClient client;
    private CountDownLatch connectionLatch;

    @Configuration
    static class TestConfig {
        @Bean
        public HandlerMapping handlerMapping() {
            Map<String, WebSocketHandler> map = new HashMap<>();
            map.put("/ws", session -> {
                Mono<Void> input = session.receive()
                    .map(WebSocketMessage::getPayloadAsText)
                    .map(message -> {
                        if (message.contains("getHealth")) {
                            return "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"ok\"}";
                        }
                        return "pong";
                    })
                    .map(session::textMessage)
                    .as(session::send);
                return input;
            });

            SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
            mapping.setUrlMap(map);
            mapping.setOrder(-1);
            return mapping;
        }

        @Bean
        public WebSocketHandlerAdapter handlerAdapter() {
            return new WebSocketHandlerAdapter(new HandshakeWebSocketService());
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        connectionLatch = new CountDownLatch(1);
        String wsUrl = String.format("ws://localhost:%d/ws", port);
        client = SubscriptionWebSocketClient.getExactPathInstance(wsUrl);
        assertTrue(client.waitForConnection(5, TimeUnit.SECONDS), "Connection timed out");
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.cleanSubscriptions();
            client.close();
        }
    }

    @Test
    void testConnectionEstablished() {
        assertTrue(client.isOpen(), "WebSocket should be open");
    }

    @Test
    void testSendAndReceiveMessage() {
        String testMessage = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getHealth\"}";
        client.send(testMessage);

        StepVerifier.create(client.getMessageFlux().next())
            .assertNext(message -> {
                assertNotNull(message, "Received message should not be null");
                assertTrue(message.contains("result"), "Response should contain result");
            })
            .verifyComplete();
    }

    @Test
    void testConnectionCloseAndReconnect() throws Exception {
        client.close();
        assertFalse(client.isOpen(), "WebSocket should be closed");

        String wsUrl = String.format("ws://localhost:%d/ws", port);
        client = SubscriptionWebSocketClient.getExactPathInstance(wsUrl);
        assertTrue(client.waitForConnection(5, TimeUnit.SECONDS), "Reconnection timed out");
        assertTrue(client.isOpen(), "WebSocket should be open after reconnection");
    }
}