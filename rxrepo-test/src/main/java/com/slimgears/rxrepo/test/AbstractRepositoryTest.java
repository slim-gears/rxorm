package com.slimgears.rxrepo.test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.EntitySet;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.AnnotationRulesJUnit;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Maybe;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.CompletableSubject;
import org.junit.*;
import org.junit.rules.MethodRule;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public abstract class AbstractRepositoryTest {
    @Rule public final TestName testNameRule = new TestName();
    @Rule public final MethodRule annotationRules = AnnotationRulesJUnit.rule();

    private Repository repository;

    @Before
    public void setUp() {
        this.repository = createRepository();
        System.out.println("Starting test: " + testNameRule.getMethodName());
    }

    @After
    public void tearDown() {
        System.out.println("Test finished: " + testNameRule.getMethodName());
        this.repository.clearAndClose();
    }

    protected abstract Repository createRepository();


    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testLiveSelectThenInsert() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);

        AtomicInteger counter = new AtomicInteger();

        TestObserver<Notification<Product>> productUpdatesTest = productSet
                .query()
                //.where(Product.$.price.greaterThan(110))
                .liveSelect()
                .queryAndObserve()
                .doOnNext(n -> System.out.println("Received notifications: " + counter.incrementAndGet()))
                .doOnSubscribe(d -> System.out.println("Subscribed for live query"))
                .test();

        TestObserver<Long> productCount = productSet
                .query()
                .liveSelect()
                .count()
                .doOnNext(c -> System.out.println("Count: " + c))
                .test();

        productSet.update(Products.createMany(1000))
                .test()
                .await()
                .assertNoErrors();

        productUpdatesTest
                .awaitCount(1000);

        productSet.delete().where(Product.$.key.id.betweenExclusive(100, 130))
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertValue(29);

        productUpdatesTest
                .awaitCount(1020)
                .assertValueAt(1019, Notification::isDelete)
                .assertNoErrors();

        productCount
                .assertNoErrors();
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testAddAndRetrieveByKey() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product 1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().where(Product.$.key.eq(UniqueId.productId(1))).count().blockingGet());
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testAddAlreadyExistingObject() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product 1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().count().blockingGet());
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testAddRecursiveInventory() throws InterruptedException {
        Inventory inventory = Inventory.builder()
                .id(UniqueId.inventoryId(1))
                .name("Inventory 1")
                .inventory(Inventory.builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory 2")
                        .build())
                .build();

        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        inventorySet
                .update(inventory)
                .test()
                .await()
                .assertNoErrors();

        inventorySet.findAll()
                .sorted(Comparator.comparing(c -> c.id().id()))
                .test()
                .awaitCount(2)
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(0, inventory);
    }

    @Test
    @UseLogLevel(LogLevel.DEBUG)
    public void testAddAndRetrieveSingleEntity() {
        Product product = Products.createOne();
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        productSet.update(product)
                .ignoreElement()
                .blockingAwait();

        productSet.findAll()
                .test()
                .awaitCount(1)
                .assertNoErrors()
                .assertNoTimeout()
                .assertValue(product);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        List<Product> products = Arrays.asList(
                Product.builder()
                        .name("Product 1")
                        .key(UniqueId.productId(1))
                        .price(1001)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory 2")
                                .build())
                        .build(),
                Product.builder()
                        .name("Product 2")
                        .key(UniqueId.productId(2))
                        .price(1002)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory 2")
                                .build())
                        .build());

        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValueAt(0, items -> items.size() == 2)
                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product 1")))
                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product 2")));

        Assert.assertEquals(Long.valueOf(1), inventorySet.query().count().blockingGet());
    }

    @Test
    public void testInsertThenLiveSelectShouldReturnAdded() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(1000);
        productSet.update(products).test().await();

        productSet.query()
                .liveSelect()
                .queryAndObserve()
                .test()
                .awaitCount(1000)
                .assertValueAt(10, NotificationPrototype::isCreate);
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testInsertThenLiveSelectCountShouldReturnCount() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(1000);
        productSet.update(products).test().await();

        TestObserver<Long> countObserver = productSet.query()
                .liveSelect()
                .count()
                .test();

        countObserver
                .awaitCount(1)
                .assertValueAt(0, c -> c == 1000);

        productSet.delete()
                .where(Product.$.searchText("Product 1*"))
                .execute()
                .test()
                .await()
                .assertValue(111);

        countObserver
                .awaitCount(2)
                .assertValueAt(1, 889L);
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testAtomicUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        CompletableSubject trigger1 = CompletableSubject.create();
        CompletableSubject trigger2 = CompletableSubject.create();

        productSet.update(Products.createMany(1)).test().await().assertNoErrors();

        TestObserver<Product> prodUpdateTester1 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #1").build())
                                .delay(trigger1.toFlowable())))
                .test();

        Thread.sleep(500);

        TestObserver<Product> prodUpdateTester2 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #2").build())
                                .delay(trigger2.toFlowable())))
                .test();

        Thread.sleep(500);
        trigger1.onComplete();
        prodUpdateTester1.await().assertNoErrors();

        trigger2.onComplete();
        prodUpdateTester2
                .await()
                .assertNoErrors()
                .assertValue(p -> "Product 0 Updated name #1 Updated name #2".equals(p.name()));
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(1000);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        System.out.println("Duration: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        productSet.query()
                .where(Product.$.name.contains("21"))
                .select()
                .count()
                .test()
                .await()
                .assertValue(20L);

        productSet.query()
                .where(Product.$.name.contains("21"))
                .select(Product.$.price)
                .aggregate(Aggregator.sum())
                .test()
                .await()
                .assertValue(2364);

        productSet
                .query()
                .where(Product.$.name.contains("231"))
                .select()
                .retrieve(Product.$.key, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValue(p -> p.name() == null)
                .assertValue(p -> p.key().id() == 231)
                .assertValue(p -> p.price() == 110)
                .assertValue(p -> "Inventory 31".equals(requireNonNull(p.inventory()).name()))
                .assertValueCount(1);

        productSet
                .query()
                .where(Product.$.type.in(ProductPrototype.Type.ComputerSoftware, ProductPrototype.Type.ComputeHardware))
                .skip(66)
                .retrieve()
                .test()
                .await()
                .assertValueCount(600);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenSearch() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(100);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.searchText("Product 31"))
                .select()
                .retrieve(Product.$.key, Product.$.name, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    public void testInsertThenUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(1000);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .update()
                .set(Product.$.name, Product.$.name.concat(" - ").concat(Product.$.inventory.name.asString()))
                .where(Product.$.key.id.betweenExclusive(100, 200))
                .limit(20)
                .prepare()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(20)
                .assertValueAt(15, pr -> {
                    System.out.println(pr);
                    Matcher matcher = Pattern.compile("Product 1([0-9]+) - Inventory ([0-9]+)").matcher(requireNonNull(pr.name()));
                    return matcher.matches() && Integer.valueOf(matcher.group(1)).equals(Integer.valueOf(matcher.group(2)));
                });
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testPartialRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(10);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.price.lessOrEqual(115))
                .orderBy(Product.$.key.id)
                .retrieve(Product.$.name)
                .doOnNext(System.out::println)
                .test()
                .await()
                .assertNoErrors()
                .assertValueAt(1, p -> "Product 1".equals(p.name()));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testEntityWithListOfReferenceField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productList(ImmutableList.copyOf(Products.createMany(10)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages.findAll()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productList().size() == 10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testEntityWithStringByReferenceMapField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productMapByName(Streams
                        .fromIterable(Products.createMany(10))
                        .collect(ImmutableMap.toImmutableMap(Product::name, p -> p)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages.findAll()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productMapByName().size() == 10);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testEntityWithListOfStringField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .aliases(ImmutableList.of("p1", "p2"))
                .build();

        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        products
                .findAll()
                .test()
                .await()
                .assertValue(p -> requireNonNull(p.aliases()).size() == 2);
    }

    @Test
    public void testFilterByNestedCompoundKey() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products.createMany(10)).test().await();
        repository.entities(Product.metaClass).query()
                .where(Product.$.inventory.id.eq(UniqueId.inventoryId(0)))
                .retrieve()
                .test()
                .await()
                .assertValueCount(10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testEntityWithListOfEmbeddedField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .relatedIds(ImmutableList.of(UniqueId.storageId(3), UniqueId.inventoryId(2), UniqueId.productId(1)))
                .build();

        repository.entities(Product.metaClass)
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        repository.entities(Product.metaClass)
                .findAll()
                .take(1)
                .singleElement()
                .test()
                .await()
                .assertValue(p -> p.relatedIds().size() == 3)
                .assertValue(p -> p.relatedIds().get(0).id() == 3);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenQueryValueIn() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .await();

        repository.entities(Product.metaClass)
                .query()
                .where(Product.$.key.in(UniqueId.productId(2), UniqueId.productId(3)))
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(2);
    }

    @Test
    public void testFilteredLiveQuery() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product 1")
                .price(101)
                .build())
                .ignoreElement()
                .blockingAwait();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(99)
                .build())
                .ignoreElement()
                .blockingAwait();


        TestObserver<Notification<Product>> productObserver = products
                .query()
                .where(Product.$.price.greaterThan(100))
                .queryAndObserve()
                .doOnNext(System.out::println)
                .test();

        productObserver
                .awaitCount(1)
                .assertNoErrors()
                .assertNoTimeout()
                .assertValueAt(0, Notification::isCreate)
                .assertValueAt(0, n -> requireNonNull(n.newValue()).price() == 101);

        productObserver
                .awaitCount(2)
                .assertTimeout();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(102)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .awaitCount(2)
                .assertNoErrors()
                .assertValueCount(2)
                .assertValueAt(1, Notification::isCreate)
                .assertValueAt(1, n -> requireNonNull(n.newValue()).price() == 102);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product 1")
                .price(95)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .awaitCount(3)
                .assertNoErrors()
                .assertValueCount(3)
                .assertValueAt(2, NotificationPrototype::isDelete)
                .assertValueAt(2, n -> requireNonNull(n.oldValue()).price() == 101);

        products.update(Product
                .builder()
                .key(UniqueId.productId(3))
                .name("Product 3")
                .price(92)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .awaitCount(4)
                .assertTimeout();
    }

    @Test @UseLogLevel(LogLevel.TRACE)
    public void testDistinctSelect() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).ignoreElement().blockingAwait();
        products.query()
                .selectDistinct(Product.$.inventory.name)
                .retrieve()
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2)
                .assertValue(l -> l.contains("Inventory 0"));

        products.query()
                .selectDistinct(Product.$.inventory)
                .retrieve(Inventory.$.name)
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2);
    }

    @Test
    public void testQueryByNestedEmbeddedObject() throws InterruptedException {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).ignoreElement().blockingAwait();
        UniqueId vendorId = UniqueId.vendorId(2);
        products.query()
                .where(Product.$.vendor.id.eq(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));

        products.query()
                .where(Product.$.vendor.id.in(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testObserveAsList() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).ignoreElement().blockingAwait();
        TestObserver<List<Product>> productTestObserver = products.query()
                .orderBy(Product.$.name)
                .limit(3)
                .skip(2)
                .observeAsList()
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 10000)
                .assertNoTimeout()
                .assertValueAt(0, l -> l.size() == 3)
                .assertValueAt(0, l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValueAt(0, l -> Objects.equals(l.get(2).name(), "Product 4"));

        products.update(Arrays.asList(
                Product.builder()
                        .name("Product 3-1")
                        .key(UniqueId.productId(11))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product 1-1")
                        .key(UniqueId.productId(13))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product 5-1")
                        .key(UniqueId.productId(12))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build()))
                .ignoreElement().blockingAwait();

        productTestObserver
                .awaitCount(2, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 10000)
                .assertValueAt(1, l -> l.size() == 3)
                .assertValueAt(1, l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValueAt(1, l -> Objects.equals(l.get(1).name(), "Product 3"))
                .assertValueAt(1, l -> Objects.equals(l.get(2).name(), "Product 3-1"));
    }

    @Test
    public void testLiveQueryWithProjection() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products.createMany(10))
                .test()
                .await()
                .assertNoErrors();

        TestObserver<Notification<Product>> productNameChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name)
                .filter(NotificationPrototype::isModify)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.type)
                .filter(NotificationPrototype::isModify)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productNameAndTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name, Product.$.type)
                .filter(NotificationPrototype::isModify)
                .doOnNext(System.out::println)
                .test();

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().name(p.name() + " - new name").build()))
                .test()
                .await()
                .assertValue(p -> "Product 1 - new name".equals(p.name()))
                .assertNoErrors();

        productNameChanges.awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, 10000).assertValueCount(1);
        productNameAndTypeChanges.awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, 10000).assertValueCount(1);
        productTypeChanges.assertNoValues();

        productNameChanges
                .assertValue(NotificationPrototype::isModify)
                .assertValue(n -> "Product 1".equals(requireNonNull(n.oldValue()).name()))
                .assertValue(n -> "Product 1 - new name".equals(requireNonNull(n.newValue()).name()));

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().type(ProductPrototype.Type.ComputerSoftware).build()))
                .test()
                .await()
                .assertValue(p -> ProductPrototype.Type.ComputerSoftware.equals(p.type()))
                .assertNoErrors();

        productTypeChanges.awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, 10000).assertValueCount(1);
        productNameAndTypeChanges.awaitCount(2, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, 10000).assertValueCount(2);
        productNameChanges.assertValueCount(1);

        productTypeChanges
                .assertValue(NotificationPrototype::isModify)
                .assertValue(n -> ProductPrototype.Type.ComputeHardware.equals(requireNonNull(n.oldValue()).type()))
                .assertValue(n -> ProductPrototype.Type.ComputerSoftware.equals(requireNonNull(n.newValue()).type()));
    }

}
