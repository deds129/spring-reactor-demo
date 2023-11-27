package ru.deds.springreactordemo;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

class UsingReactiveOperationsTest {
    
    @Test
    void createReactiveOperations() {
        //create Flux
        Flux<String> stringFlux = Flux.just("Hello", "World");
        
        //add subscriber
        stringFlux.subscribe(
                f -> System.out.println("Flux just contains: " + f)
        );

        //check values in stream
        StepVerifier.create(stringFlux)
                .expectNext("Hello")
                .expectNext("World")
                .verifyComplete();
    }
    
    @Test
    void createFromArray() {
        String[] arrayOfStrings = {"Hello", "World"};
        Flux<String> stringFlux = Flux.fromArray(arrayOfStrings);
        
        StepVerifier.create(stringFlux)
                .expectNext("Hello")
                .expectNext("World")
                .verifyComplete();
    }

    @Test
    void createFromCollection() {
        List<String> strings = List.of("Hello", "World");
        Flux<String> stringFlux = Flux.fromIterable(strings);

        StepVerifier.create(stringFlux)
                .expectNext("Hello")
                .expectNext("World")
                .verifyComplete();
    }

    @Test
    void createFromStream() {
        Stream<String> strings = Stream.of("Hello", "World");
        Flux<String> stringFlux = Flux.fromStream(strings);

        StepVerifier.create(stringFlux)
                .expectNext("Hello")
                .expectNext("World")
                .verifyComplete();
    }
    
    @Test
    void generateUsingRange() {
        Flux<Integer> integerFlux = Flux.range(1, 5);
        
        StepVerifier.create(integerFlux)
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .expectNext(4)
                .expectNext(5)
                .verifyComplete();
    }
    
    @Test
    void generateUsingInterval() {
        Flux<Long> longFlux = Flux.interval(Duration.ofSeconds(1))
                .take(5);

        StepVerifier.create(longFlux)
                .expectNext(1L)
                .expectNext(2L)
                .expectNext(3L)
                .expectNext(4L)
                .verifyComplete();
    }
    
    @Test
    void mergeFluxes() {
        Flux<String> characterFlux = Flux
                .just("Batman", "Spiderman", "Tor")
                .delayElements(Duration.ofMillis(500));

        Flux<String> foodFlux = Flux
                .just("Lasagna", "Lollipops", "Apples")
                .delaySubscription(Duration.ofMillis(250)) .delayElements(Duration.ofMillis(500));
        
        Flux<String> mergedFlux = characterFlux.mergeWith(foodFlux); //Flux.merge(characterFlux, foodFlux);
        
        StepVerifier.create(mergedFlux)
                .expectNext("Batman")
                .expectNext("Lasagna")
                .expectNext("Spriderman")
                .expectNext("Lollipops")
                .expectNext("Tor")
                .expectNext("Apples")
               .verifyComplete();
    }
    
    @Test
    void zipFluxes() {
        Flux<String> characterFlux = Flux
                .just("Batman", "Spiderman", "Tor");
        Flux<String> foodFlux = Flux
                .just("Lasagna", "Lollipops", "Apples");

        Flux<Tuple2<String, String>> zippedFlux = Flux.zip(characterFlux, foodFlux);

        StepVerifier.create(zippedFlux) .expectNextMatches(p ->
                p.getT1().equals("Garfield") &&
                        p.getT2().equals("Lasagna")) .expectNextMatches(p ->
                p.getT1().equals("Kojak") &&
                        p.getT2().equals("Lollipops")) .expectNextMatches(p ->
                p.getT1().equals("Barbossa") &&
                        p.getT2().equals("Apples")) .verifyComplete();
    }

    @Test
    public void zipFluxesToObject() {
        Flux<String> characterFlux = Flux .just("Garfield", "Kojak", "Barbossa");
        Flux<String> foodFlux = Flux .just("Lasagna", "Lollipops", "Apples");
        Flux<String> zippedFlux =
                Flux.zip(characterFlux, foodFlux, (c, f) -> c + " eats " + f); // create new flux
        StepVerifier.create(zippedFlux) .expectNext("Garfield eats Lasagna") .expectNext("Kojak eats Lollipops") .expectNext("Barbossa eats Apples") .verifyComplete();
    }

    @Test
    public void firstWithSignalFlux() {
        Flux<String> slowFlux = Flux.just("tortoise", "snail", "sloth") .delaySubscription(Duration.ofMillis(100));
        Flux<String> fastFlux = Flux.just("hare", "cheetah", "squirrel");
        Flux<String> firstFlux = Flux.firstWithSignal(slowFlux, fastFlux);
        StepVerifier.create(firstFlux) .expectNext("hare") .expectNext("cheetah") .expectNext("squirrel") .verifyComplete();
    }
    
    
}
