package com.myway.reactive.example.reactor;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;


@Service
public class TimeBasedDemoService {


    public Mono<String> withTimeout(Mono<String> source,
                                    Duration timeout,
                                    String fallback) {
        return source
                .timeout(timeout)
                .onErrorReturn(TimeoutException.class, fallback);
    }


    public Flux<String> slowEmission(List<String> items) {
        return Flux.fromIterable(items).delayElements(Duration.ofSeconds(1));
    }


    public Mono<String> withBackoff(Mono<String> source) {
        return source.retryWhen(
                Retry.backoff(3, Duration.ofSeconds(10)
                ).jitter(0.0));
    }

    /** Polls every 30 seconds, 3 times. */
    public Flux<Long> poll() {
        return Flux.interval(Duration.ofSeconds(30)).take(3);
    }
}
