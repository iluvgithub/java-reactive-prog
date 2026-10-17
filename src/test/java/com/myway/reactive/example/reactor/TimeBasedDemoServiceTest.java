package com.myway.reactive.example.reactor;

import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every test here uses StepVerifier.withVirtualTime: the clock is simulated,
 * so "hours" pass instantly and the whole class runs in milliseconds.
 * <p>
 * Remember: the publisher MUST be created inside the supplier lambda.
 */
class TimeBasedDemoServiceTest {

    private final TimeBasedDemoService service = new TimeBasedDemoService();

    // ------------------------------------------------------------ timeout

    @Test
    void timeout_returnsFallbackWhenSourceIsTooSlow() {
        // arrange
        Mono<String> monoString = service.withTimeout(
                Mono.delay(Duration.ofSeconds(10)).thenReturn("slow"),
                Duration.ofSeconds(2),
                "fallback");
        // act && assert
        StepVerifier.withVirtualTime(() -> monoString)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))   // nothing yet after 1s
                .thenAwait(Duration.ofSeconds(1))       // t = 2s: timeout fires
                .expectNext("fallback")
                .verifyComplete();
    }

    @Test
    void timeout_returnsSourceValueWhenFastEnough() {
        // arrange
        Mono<String> monoString = service.withTimeout(
                Mono.delay(Duration.ofSeconds(1)).thenReturn("fast"),
                Duration.ofSeconds(2),
                "fallback");
        // act && assert
        StepVerifier.withVirtualTime(() -> monoString)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("fast")
                .verifyComplete();
    }

    // ------------------------------------------------------ delayElements

    @Test
    void slowEmission_emitsOneItemPerSecond() {
        // arrange
        Flux<String> flux = service.slowEmission(List.of("a", "b", "c"));
        // act && assert
        StepVerifier.withVirtualTime(() -> flux)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1)).expectNext("a")
                .thenAwait(Duration.ofSeconds(1)).expectNext("b")
                .thenAwait(Duration.ofSeconds(1)).expectNext("c")
                .verifyComplete();
    }

    // ------------------------------------------------------------ backoff

    @Test
    void backoff_succeedsOnThirdAttempt() {
        // arrange
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> mono = Mono.defer(() ->
                attempts.incrementAndGet() < 3
                        ? Mono.<String>error(new IllegalStateException("fail"))
                        : Mono.just("ok"));
        Mono<String> retry = service.withBackoff(mono);
        // act && assert
        StepVerifier.withVirtualTime(() -> retry)
                .expectSubscription()
                // t=0 attempt 1 fails, t=10s attempt 2 fails, t=30s attempt 3 succeeds
                .expectNoEvent(Duration.ofSeconds(29))
                .then(() -> assertThat(attempts.get()).isEqualTo(2))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("ok")
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void backoff_failsWhenRetriesAreExhausted() {
        // arrange
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> mono = Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.<String>error(new IllegalStateException("always fails"));
        });
        Mono<String> retry = service.withBackoff(mono);
        // act && assert
        StepVerifier.withVirtualTime(() -> retry)
                .expectSubscription()
                // Delays: 10s + 20s + 40s = 70s, then the retries are exhausted
                .thenAwait(Duration.ofSeconds(70))
                .expectErrorMatches(Exceptions::isRetryExhausted)
                .verify();

        assertThat(attempts.get()).isEqualTo(4);   // 1 initial attempt + 3 retries
    }

    // ------------------------------------------------------------ interval

    @Test
    void poll_emitsEvery30Seconds() {
        // arrange
        Flux<Long> flux = service.poll();
        // act && assert
        StepVerifier.withVirtualTime(() -> flux)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(29))
                .thenAwait(Duration.ofSeconds(1)).expectNext(0L)
                .thenAwait(Duration.ofSeconds(30)).expectNext(1L)
                .thenAwait(Duration.ofSeconds(30)).expectNext(2L)
                .verifyComplete();
    }
}
