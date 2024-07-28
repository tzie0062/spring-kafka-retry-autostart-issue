package com.example.kafkaretry;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static com.example.kafkaretry.KafkaRetryApplication.LISTENER_ID_FOO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
@Testcontainers
@SpringBootTest
@Import(AutoStartTrueTest.TestConfig.class)
class AutoStartTrueTest {
    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withStartupTimeout(Duration.ofSeconds(20L));
    @Autowired
    protected FooKafkaListener fooKafkaListener;
    @Autowired
    protected KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.topic.foo}")
    protected String topicFoo;

    @Test
    void retryWorks() throws InterruptedException {
        kafkaTemplate.send(topicFoo, "test-key", "test-value");
        await()
                .atMost(Duration.ofSeconds(10L))
                .until(() -> fooKafkaListener.isEventProcessed());
        assertThat(fooKafkaListener.getAttemptCounter()).isEqualTo(3);
    }

    private static class TestApplicationListener {
        @Autowired
        protected KafkaListenerEndpointRegistry endpointRegistry;

        @EventListener(ApplicationReadyEvent.class)
        public void startKafkaListener() {
            MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(LISTENER_ID_FOO);
            if (!listenerContainer.isRunning()) {
                log.info("Listener not running - starting.");
                listenerContainer.start();
            }
        }
    }

    @TestConfiguration
    public static class TestConfig {
        @Bean
        @ConditionalOnProperty(name = "autoStartup.enabled", havingValue = "false")
        TestApplicationListener testApplicationListener() {
            return new TestApplicationListener();
        }
    }

}
