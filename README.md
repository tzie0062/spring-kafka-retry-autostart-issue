# Sample project to show a spring-kafka-retry issue

## Problem
When a `@KafkaListener` is started automatically, spring-kafka's non-blocking retry works as expected. However if the listener is manually started (`autoStartup=false`), this does not seem to be the case.

Tested with `spring boot 3.3.2` / `spring-kafka 3.2.2`

## Testing
Run the application by calling: 
```
./mvnw clean verify -DautoStartup.enabled=true
```
to test retries with `autoStartup` enabled: the test will pass. 

Running:
```
./mvnw clean verify -DautoStartup.enabled=false
```
will launch the `@KafkaListener` with `autoStartup` disabled. An `@EventListener` is used to start the listener once the application has started.
Here the test will not pass, since no retries are happening.
