package com.example.kafkaretry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import static com.example.kafkaretry.KafkaRetryApplication.LISTENER_ID_FOO;
import static org.springframework.kafka.retrytopic.RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS;

@Slf4j
@RequiredArgsConstructor
public class FooKafkaListener {
    @Getter
    private boolean eventProcessed;
    @Getter
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
