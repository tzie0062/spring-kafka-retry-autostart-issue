package com.example.kafkaretry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;

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
}
