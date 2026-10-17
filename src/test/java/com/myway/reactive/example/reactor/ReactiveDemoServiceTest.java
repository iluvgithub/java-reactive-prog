package com.myway.reactive.example.reactor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

class ReactiveDemoServiceTest {

    private ReactiveDemoService service;

    @BeforeEach
    void setUp() {
        service = new ReactiveDemoService();
    }

    // ---------------------------------------------------------------- Mono

    @Test
    void greetEmitsGreeting() {
        StepVerifier.create(service.greet("Pascal"))
                .expectNext("Hello, Pascal!")
                .verifyComplete();
    }

    @Test
    void findNicknameIsEmptyForUnknownName() {
        StepVerifier.create(service.findNickname("bob"))
                .verifyComplete();
    }

    @Test
    void findNicknameReturnsNicknameCaseInsensitively() {
        StepVerifier.create(service.findNickname("PASCAL"))
                .expectNext("Pasc")
                .verifyComplete();
    }

    @Test
    void nicknameOrDefaultFallsBackWhenEmpty() {
        StepVerifier.create(service.nicknameOrDefault("bob"))
                .expectNext("no-nickname")
                .verifyComplete();
    }

    @Test
    void greetingLengthMatchesGreeting() {
        StepVerifier.create(service.greetingLength("Bob"))
                .expectNext("Hello, Bob!".length())
                .verifyComplete();
    }

    @Test
    void shoutGreetingUpperCases() {
        StepVerifier.create(service.shoutGreeting("Bob"))
                .expectNext("HELLO, BOB!")
                .verifyComplete();
    }

    @Test
    void parseOrMinusOneParsesValidNumber() {
        StepVerifier.create(service.parseOrMinusOne("42"))
                .expectNext(42)
                .verifyComplete();
    }

    @Test
    void parseOrMinusOneRecoversFromInvalidNumber() {
        StepVerifier.create(service.parseOrMinusOne("abc"))
                .expectNext(-1)
                .verifyComplete();
    }

    @Test
    void fullNameZipsBothMonos() {
        StepVerifier.create(service.fullName(Mono.just("Ada"), Mono.just("Lovelace")))
                .expectNext("Ada Lovelace")
                .verifyComplete();
    }

    // ---------------------------------------------------------------- Flux

    @Test
    void numbersEmitsRange() {
        StepVerifier.create(service.numbers(3))
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    @Test
    void squaresOfEvenNumbersFiltersThenMaps() {
        StepVerifier.create(service.squaresOfEvenNumbers(6))
                .expectNext(4, 16, 36)
                .verifyComplete();
    }

    @Test
    void splitIntoLettersFlattensWords() {
        StepVerifier.create(service.splitIntoLetters(List.of("ab", "c")))
                .expectNext("a", "b", "c")
                .verifyComplete();
    }

    @Test
    void concatenatedPreservesOrder() {
        StepVerifier.create(service.concatenated(Flux.just("a", "b"), Flux.just("c")))
                .expectNext("a", "b", "c")
                .verifyComplete();
    }

    @Test
    void sumReducesFlux() {
        StepVerifier.create(service.sum(4))
                .expectNext(10)
                .verifyComplete();
    }

    @Test
    void collectGathersIntoList() {
        StepVerifier.create(service.collect(3))
                .expectNext(List.of(1, 2, 3))
                .verifyComplete();
    }

    @Test
    void numbersWithRecoveryEmitsFallbackAfterError() {
        StepVerifier.create(service.numbersWithRecovery())
                .expectNext(1, 2, 3)
                .expectNext(-1, -2)
                .verifyComplete();
    }

    @Test
    void batchesGroupsElementsBySize() {
        StepVerifier.create(service.batches(5, 2))
                .expectNext(List.of(1, 2))
                .expectNext(List.of(3, 4))
                .expectNext(List.of(5))
                .verifyComplete();
    }

    // ------------------------------------------------- time-based (virtual time)

    /**
     * withVirtualTime takes a Supplier so the Flux is created AFTER the virtual
     * scheduler is installed; otherwise Flux.interval would capture the real
     * scheduler and the test would wait in real time (or hang).
     */
    @Test
    void ticksEmitsOneValuePerPeriod() {
        StepVerifier.withVirtualTime(() -> service.ticks(Duration.ofSeconds(1), 3))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(999))   // nothing before the first period
                .thenAwait(Duration.ofMillis(1))         // t = 1s
                .expectNext(0L)
                .expectNoEvent(Duration.ofMillis(999))   // nothing between ticks
                .thenAwait(Duration.ofMillis(1))         // t = 2s
                .expectNext(1L)
                .thenAwait(Duration.ofSeconds(1))        // t = 3s
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    void ticksWithLongPeriodRunsInstantlyInRealTime() {
        // 3 hours of virtual time, milliseconds of real time.
        StepVerifier.withVirtualTime(() -> service.ticks(Duration.ofHours(1), 3))
                .expectSubscription()
                .expectNoEvent(Duration.ofMinutes(59))
                .thenAwait(Duration.ofHours(3))
                .expectNext(0L, 1L, 2L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));          // real-time safety net against hangs
    }

    @Test
    void ticksStopsAfterRequestedCount() {
        StepVerifier.withVirtualTime(() -> service.ticks(Duration.ofMinutes(5), 2))
                .thenAwait(Duration.ofMinutes(10))
                .expectNextCount(2)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    void ticksDoesNotEmitBeforeFirstPeriodElapses() {
        StepVerifier.withVirtualTime(() -> service.ticks(Duration.ofSeconds(10), 5))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(9))
                .thenCancel()                            // we only care that nothing arrived early
                .verify(Duration.ofSeconds(2));
    }
}
