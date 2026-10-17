package com.myway.reactive.example.reactor;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Small service illustrating the core Project Reactor types:
 * <ul>
 *   <li>{@link Mono}: a publisher emitting 0 or 1 element</li>
 *   <li>{@link Flux}: a publisher emitting 0..N elements</li>
 * </ul>
 */
@Service
public class ReactiveDemoService {

    // ---------------------------------------------------------------- Mono
     public Mono<String> greet(String name) { // Mono that emits a single value
        return Mono.just("Hello, " + name + "!");
    }

    /** Mono that may be empty: Mono.justOrEmpty handles null safely. */
    public Mono<String> findNickname(String name) {
        return Mono.justOrEmpty("pascal".equalsIgnoreCase(name) ? "Pasc" : null);
    }

    /** Mono with a fallback value when empty. */
    public Mono<String> nicknameOrDefault(String name) {
                return findNickname(name).defaultIfEmpty("no-nickname");
    }

    /** Mono.map (synchronous transform) vs flatMap (asynchronous, returns a Mono). */
    public Mono<Integer> greetingLength(String name) {
        return greet(name).map(String::length);
    }

    public Mono<String> shoutGreeting(String name) {
        return greet(name).flatMap(g -> Mono.just(g.toUpperCase()));
    }

    /** Error handling: the error is converted to a fallback value. */
    public Mono<Integer> parseOrMinusOne(String value) {
        return Mono.fromSupplier(() -> Integer.parseInt(value))
                .onErrorReturn(NumberFormatException.class, -1);
    }

    /** Combining two Monos with zip. */
    public Mono<String> fullName(Mono<String> firstName, Mono<String> lastName) {
        return Mono.zip(firstName, lastName, (f, l) -> f + " " + l);
    }

    // ---------------------------------------------------------------- Flux

    /** Flux emitting a range of integers. */
    public Flux<Integer> numbers(int count) {
        return Flux.range(1, count);
    }

    /** Typical pipeline: filter + map. */
    public Flux<Integer> squaresOfEvenNumbers(int count) {
        return numbers(count)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n);
    }

    /** Flux from a collection, with flatMap producing a Flux per element. */
    public Flux<String> splitIntoLetters(List<String> words) {
        return Flux.fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")));
    }

    /** concatWith preserves order: first all elements of A, then all of B. */
    public Flux<String> concatenated(Flux<String> a, Flux<String> b) {
        return a.concatWith(b);
    }

    /** Reducing a Flux into a Mono. */
    public Mono<Integer> sum(int count) {
        return numbers(count).reduce(0, Integer::sum);
    }

    /** Collecting a Flux into a Mono of List. */
    public Mono<List<Integer>> collect(int count) {
        return numbers(count).collectList();
    }

    /** Flux that fails midway and recovers with a fallback Flux. */
    public Flux<Integer> numbersWithRecovery() {
        return Flux.range(1, 5)
                .map(n -> {
                    if (n == 4) {
                        throw new IllegalStateException("boom at " + n);
                    }
                    return n;
                })
                .onErrorResume(IllegalStateException.class, e -> Flux.just(-1, -2));
    }

    /** Time-based Flux: one element every 'period'. Use StepVerifier.withVirtualTime to test it. */
    public Flux<Long> ticks(Duration period, int count) {
        return Flux.interval(period).take(count);
    }

    /** Backpressure-friendly batching. */
    public Flux<List<Integer>> batches(int count, int size) {
        return numbers(count).buffer(size);
    }
}
