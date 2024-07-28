package com.example.kafkaretry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;

@Slf4j
@SpringBootApplication
public class KafkaRetryApplication {
    public static final String LISTENER_ID_FOO = "FooKafkaListenerFoo";

    public static void main(String[] args) {
        SpringApplication.run(KafkaRetryApplication.class, args);
    }

    @Bean
    public FooKafkaListener fooKafkaListener() {
        return new FooKafkaListener();
    }

    @Bean
    public RetryTopicConfiguration fooRetryTopicConfiguration(KafkaTemplate<String, String> kafkaTemplate, @Value("${kafka.topic.foo}") String topic) {
        return RetryTopicConfigurationBuilder.newInstance()
                .fixedBackOff(1000L)
                .includeTopic(topic)
                .retryTopicSuffix("-retry")
                .maxAttempts(3)
                .create(kafkaTemplate);
    }

    @Slf4j
    @RequiredArgsConstructor
    @Getter
    public static class FooKafkaListener {
        private boolean eventProcessed;
        private Integer attemptCounter;

        @KafkaListener(id = LISTENER_ID_FOO, topics = "${kafka.topic.foo}", groupId = "my-group-id", autoStartup = "${autoStartup.enabled}")
        public void processEvent(@Payload String event,
                                 @Header(name = KafkaHeaders.RECEIVED_KEY, required = false, defaultValue = "n/a") String key,
                                 @Header(name = DEFAULT_HEADER_ATTEMPTS, required = false, defaultValue = "1") int attempt) {
            try {
                log.info("Received event {} with key {} - attempt: {}", event, key, attempt);
                this.attemptCounter = attempt;
                if (attemptCounter < 3) {
                    throw new RuntimeException("Error " + attemptCounter);
                }
                eventProcessed = true;
            } catch (Exception ex) {
                log.warn("Exception during attempt {}", attempt, ex.getMessage());
                throw ex;
            }
        }
    }
}
