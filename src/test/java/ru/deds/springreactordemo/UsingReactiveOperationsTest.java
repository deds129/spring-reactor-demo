package ru.deds.springreactordemo;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
                .expectNext(0L)
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
                .delaySubscription(Duration.ofMillis(250)).delayElements(Duration.ofMillis(500));

        Flux<String> mergedFlux = characterFlux.mergeWith(foodFlux); //Flux.merge(characterFlux, foodFlux);

        //order is not guaranteed
        StepVerifier.create(mergedFlux)
                .expectNext("Batman")
                .expectNext("Lasagna")
                .expectNext("Spiderman")
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

        //compare values between tuple values
        StepVerifier.create(zippedFlux).expectNextMatches(p ->
                p.getT1().equals("Batman") &&
                        p.getT2().equals("Lasagna")).expectNextMatches(p ->
                p.getT1().equals("Spiderman") &&
                        p.getT2().equals("Lollipops")).expectNextMatches(p ->
                p.getT1().equals("Tor") &&
                        p.getT2().equals("Apples")).verifyComplete();
    }

    @Test
    void zipFluxesToObject() {
        Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa");
        Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apples");

        //zip two fluxes using function into new flux
        Flux<String> zippedFlux =
                Flux.zip(characterFlux, foodFlux, (c, f) -> c + " eats " + f); // create new flux
        StepVerifier.create(zippedFlux).expectNext("Garfield eats Lasagna").expectNext("Kojak eats Lollipops").expectNext("Barbossa eats Apples").verifyComplete();
    }

    @Test
    void firstWithSignalFlux() {
        Flux<String> slowFlux = Flux.just("tortoise", "snail", "sloth").delaySubscription(Duration.ofMillis(100));
        Flux<String> fastFlux = Flux.just("hare", "cheetah", "squirrel");

        //get first(fastest) value between slowFlux or fastFlux
        Flux<String> firstFlux = Flux.firstWithSignal(slowFlux, fastFlux);
        StepVerifier.create(firstFlux)
                .expectNext("hare")
                .expectNext("cheetah")
                .expectNext("squirrel")
                .verifyComplete();
    }

    @Test
    void skipFirstNElementsFlux() {
        //skip first 3 elements
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five")
                .skip(3);

        StepVerifier.create(stringFlux)
                .expectNext("four")
                .expectNext("five")
                .verifyComplete();
    }

    @Test
    void skipElementsSomeDuration() {

        //start receive elements after 4 sec ((0), 1, 2, 3, 4)
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five")
                .delayElements(Duration.ofSeconds(1))
                .skip(Duration.ofSeconds(4));

        StepVerifier.create(stringFlux)
                .expectNext("four")
                .expectNext("five")
                .verifyComplete();
    }

    @Test
    void takeFirstNElementsFlux() {
        //take first 3 elements
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five")
                .take(3);

        StepVerifier.create(stringFlux)
                .expectNext("one")
                .expectNext("two")
                .expectNext("three")
                .verifyComplete();
    }

    @Test
    void takeElementsSomeDuration() {

        //start elements before reach 3 sec
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five")
                .delayElements(Duration.ofSeconds(1))
                .take(Duration.ofMillis(3500));

        StepVerifier.create(stringFlux)
                .expectNext("one")
                .expectNext("two")
                .expectNext("three")
                .verifyComplete();
    }

    @Test
    void filterFlux() {
        //fitter, skip elements with length less than 3
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five")
                .filter(s -> s.length() > 3);

        StepVerifier.create(stringFlux)
                .expectNext("three")
                .expectNext("four")
                .expectNext("five")
                .verifyComplete();
    }

    @Test
    void testDistinctFlux() {
        //exclude duplicates
        Flux<String> stringFlux = Flux.just("one", "two", "three", "four", "five", "one")
                .distinct();

        StepVerifier.create(stringFlux)
                .expectNext("one")
                .expectNext("two")
                .expectNext("three")
                .expectNext("four")
                .expectNext("five")
                .verifyComplete();
    }

    @Test
    void testMapFlux() {
        Flux<String> stringFlux = Flux.just("one apple", "two bananas", "three potatoes", "four", "five orange")
                .map(s -> s.split(" ")[0]);

        StepVerifier.create(stringFlux)
                .expectNext("one")
                .expectNext("two")
                .expectNext("three")
                .expectNext("four")
                .expectNext("five")
                .verifyComplete();
    }

    @Test
    void testFlatMapFlux() {
        //split each element to words and then take first word
        Flux<String> stringFlux = Flux.just("one apple", "two bananas", "three potatoes", "four", "five orange")
                .flatMap(n -> Mono.just(n.split(" ")[0]))
                .subscribeOn(Schedulers.parallel());

        StepVerifier.create(stringFlux)
                .expectNextMatches(p -> p.equals("one"))
                .expectNextMatches(p -> p.equals("two"))
                .expectNextMatches(p -> p.equals("three"))
                .expectNextMatches(p -> p.equals("four"))
                .expectNextMatches(p -> p.equals("five"))
                .verifyComplete();
    }

    @Test
    void buffer() {
        //strings join to new collections which contains 3 elements
        Flux<String> fruitFlux = Flux.just(
                "apple", "orange", "banana", "kiwi", "strawberry");
        Flux<List<String>> bufferedFlux = fruitFlux.buffer(3);
        StepVerifier
                .create(bufferedFlux)
                .expectNext(Arrays.asList("apple", "orange", "banana"))
                .expectNext(Arrays.asList("kiwi", "strawberry"))
                .verifyComplete();
    }

    @Test
    //buffer and then map to uppercase in parallel
    void bufferAndFlatMap() {
        Flux.just(
                        "apple", "orange", "banana", "kiwi", "strawberry").buffer(3)
                .flatMap(x ->
                        Flux.fromIterable(x)
                                .map(y -> y.toUpperCase()).subscribeOn(Schedulers.parallel()).log()
                ).subscribe();
    }

    @Test
    void collectList() {
        Flux<String> fruitFlux = Flux.just(
                "apple", "orange", "banana", "kiwi", "strawberry");
        Mono<List<String>> fruitListMono = fruitFlux.collectList();
        StepVerifier
                .create(fruitListMono)
                .expectNext(Arrays.asList(
                        "apple", "orange", "banana", "kiwi", "strawberry")) .verifyComplete();
    }
    
    @Test
    public void collectMap() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");
        Mono<Map<Character, String>> animalMapMono =
                animalFlux.collectMap(a -> a.charAt(0));
        StepVerifier .create(animalMapMono).expectNextMatches(map -> map.size() == 3 &&
                map.get('a').equals("aardvark") &&
                map.get('e').equals("eagle") &&
                map.get('k').equals("kangaroo"))
                .verifyComplete();
    }

    @Test
    public void all() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");
        Mono<Boolean> hasAMono = animalFlux.all(a -> a.contains("a")); StepVerifier.create(hasAMono)
                .expectNext(true).verifyComplete();
        Mono<Boolean> hasKMono = animalFlux.all(a -> a.contains("k")); StepVerifier.create(hasKMono)
                .expectNext(false)
                .verifyComplete();
    }
    
    @Test
    void any() {
        Flux<String> animalFlux = Flux.just(
                "aardvark", "elephant", "koala", "eagle", "kangaroo");
        Mono<Boolean> hasTMono = animalFlux.any(a -> a.contains("t"));
        StepVerifier.create(hasTMono)
                .expectNext(true) .verifyComplete();
        Mono<Boolean> hasZMono = animalFlux.any(a -> a.contains("z"));
        StepVerifier.create(hasZMono)
                .expectNext(false)
                .verifyComplete();
    }


}
