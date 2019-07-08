package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.*;
import com.slimgears.rxrepo.sql.CacheSchemaProviderDecorator;
import com.slimgears.rxrepo.sql.SchemaProvider;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.AnnotationRulesJUnit;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Maybe;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.CompletableSubject;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
@RunWith(AnnotationRulesJUnit.class)
//@UseLogLevel(UseLogLevel.Level.FINE)
public class OrientDbQueryProviderTest {
    @Rule public final TestName testNameRule = new TestName();
    private static final RepositoryConfiguration repositoryConfig = new RepositoryConfiguration() {
        @Override
        public int retryCount() {
            return 10;
        }

        @Override
        public int debounceTimeoutMillis() {
            return 1000;
        }

        @Override
        public int retryInitialDurationMillis() {
            return 10;
        }
    };

    private static final String dbName = "testDb";
    private static OServer server;
    private Repository repository;
    private OrientDB dbClient;
    private Supplier<ODatabaseDocument> dbSessionSupplier;

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = OServerMain.create(true);
        server.startup(new OServerConfiguration());
        //((Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.ALL);
    }

    @AfterClass
    public static void tearDownClass() {
        server.shutdown();
    }

    @Before
    public void setUp() {
        System.err.println("Starting test: " + testNameRule.getMethodName());
        dbClient = new OrientDB("embedded:testDbServer", OrientDBConfig.defaultConfig());
        dbClient.create(dbName, ODatabaseType.MEMORY);
        dbSessionSupplier = () -> dbClient.open(dbName, "admin", "admin");
        repository = OrientDbRepository
                .builder(dbSessionSupplier)
                .scheduler(Schedulers.io())
                .buildRepository(repositoryConfig);
    }

    @After
    public void tearDown() {
        dbClient.drop(dbName);
        dbClient.close();
        System.err.println("Test finished: " + testNameRule.getMethodName());
    }

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

        productSet.update(createProducts(1000))
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
        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        inventorySet
                .update(Inventory.builder()
                        .id(UniqueId.inventoryId(1))
                        .name("Inventory 1")
                        .inventory(Inventory.builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory 2")
                                .build())
                        .build())
                .test()
                .await()
                .assertNoErrors();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        productSet
                .update(Arrays.asList(
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
                                .build()
                ))
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
        Iterable<Product> products = createProducts(1000);
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
        Iterable<Product> products = createProducts(1000);
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

        productSet.update(createProducts(1)).test().await().assertNoErrors();

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
        Iterable<Product> products = createProducts(1000);
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
                .assertValue(p -> "Inventory 31".equals(Objects.requireNonNull(p.inventory()).name()))
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
        Iterable<Product> products = createProducts(100);
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
        Iterable<Product> products = createProducts(1000);
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
                    Matcher matcher = Pattern.compile("Product 1([0-9]+) - Inventory ([0-9]+)").matcher(Objects.requireNonNull(pr.name()));
                    return matcher.matches() && Integer.valueOf(matcher.group(1)).equals(Integer.valueOf(matcher.group(2)));
                });
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testPartialRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = createProducts(10);
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
                .productList(ImmutableList.copyOf(createProducts(10)))
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
                        .fromIterable(createProducts(10))
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
                .assertValue(p -> Objects.requireNonNull(p.aliases()).size() == 2);
    }

    @Test
    public void testFilterByNestedCompoundKey() throws InterruptedException {
        repository.entities(Product.metaClass).update(createProducts(10)).test().await();
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
                .update(createProducts(10))
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
                .blockingGet();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(99)
                .build())
                .blockingGet();


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
                .assertValueAt(0, n -> n.newValue().price() == 101);

        productObserver
                .awaitCount(2)
                .assertTimeout();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(102)
                .build())
                .blockingGet();

        productObserver
                .awaitCount(2)
                .assertNoErrors()
                .assertValueCount(2)
                .assertValueAt(1, Notification::isCreate)
                .assertValueAt(1, n -> n.newValue().price() == 102);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product 1")
                .price(95)
                .build())
                .blockingGet();

        productObserver
                .awaitCount(3)
                .assertNoErrors()
                .assertValueCount(3)
                .assertValueAt(2, NotificationPrototype::isDelete)
                .assertValueAt(2, n -> n.oldValue().price() == 101);

        products.update(Product
                .builder()
                .key(UniqueId.productId(3))
                .name("Product 3")
                .price(92)
                .build())
                .blockingGet();

        productObserver
                .awaitCount(4)
                .assertTimeout();
    }

    @Test @UseLogLevel(LogLevel.TRACE)
    public void testDistinctSelect() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(createProducts(20)).blockingGet();
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
        products.update(createProducts(20)).blockingGet();
        UniqueId vendorId = UniqueId.vendorId(2);
        products.query()
                .where(Product.$.vendor.id.eq(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> p.vendor().id().equals(vendorId));

        products.query()
                .where(Product.$.vendor.id.in(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> p.vendor().id().equals(vendorId));
    }

    private void rawDbTest(Consumer<ODatabaseSession> test) throws Exception {
        try (OrientDB dbClient = new OrientDB("embedded:testDbServer", OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            try (ODatabaseSession dbSession = dbClient.open(dbName, "admin", "admin")) {
                test.accept(dbSession);
            }
        }
    }

    @Test @Ignore
    public void testQueryByObject() throws Exception {
        rawDbTest(dbSession -> {
            OClass oClass = dbSession.createClass("MyClass");
            oClass.createProperty("myField", OType.EMBEDDED);
            OElement element = dbSession.newElement();
            element.setProperty("id", 1);
            element.setProperty("test", "testVal");
            dbSession.command("insert into MyClass set myField = ?", element)
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Inserted element: " + el));
            dbSession.query("select from MyClass where myField like " + element.toJSON())
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Received element: " + el));
        });
    }

    @Test @Ignore
    public void testAddThenQueryRawOrientDb() throws Exception {
        rawDbTest(dbSession -> {
                OClass oClass = dbSession.createClass("MyClass");
                oClass.createProperty("myField", OType.EMBEDDED);

                dbSession.command("insert into MyClass set myField = {'id': 1, '@type': 'd'}")
                        .stream()
                        .map(OResult::toJSON)
                        .forEach(el -> System.out.println("Inserted element: " + el));

                dbSession.command("insert into MyClass set myField = ?",
                        new ODocument().field("id", 1))
                        .stream()
                        .map(OResult::toJSON)
                        .forEach(el -> System.out.println("Inserted element: " + el));

                dbSession.query("select from MyClass where myField = {'id': 1, '@type': 'd', '@version': 0}")
                        .stream()
                        .map(OResult::toJSON)
                        .forEach(el -> System.out.println("Retrieved element (with filter): " + el));

                dbSession.query("select from MyClass where myField = ?",
                        new ODocument().field("id", 1))
                        .stream()
                        .map(OResult::toJSON)
                        .forEach(el -> System.out.println("Retrieved element (with filter): " + el));
        });
    }

    @Test @Ignore
    public void testCustomIndex() throws Exception {
        rawDbTest(session -> {
                OClass oClass = session.createClass("MyClass");
                oClass.createProperty("key", OType.STRING);
                session.getMetadata().getIndexManager().createIndex(
                        "MyClass.keyIndex",
                        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name(),
                        new ORuntimeKeyIndexDefinition<>(OBinaryTypeSerializer.ID),
                        null,
                        null,
                        null);
                session.command("update MyClass set key = ? upsert return after where (key = ?)", UniqueId.productId(2), UniqueId.productId(2))
                        .close();

                session.command("update MyClass set key = ? upsert return after where (key = ?)", UniqueId.productId(2), UniqueId.productId(2))
                        .stream()
                        .close();

                session.query("select from MyClass where key = ?", UniqueId.productId(2))
                        .stream()
                        .map(OResult::toJSON)
                        .forEach(System.out::println);
        });
    }

    @Test @Ignore
    @UseLogLevel(LogLevel.TRACE)
    public void testRawOrientDbLuceneIndex() throws InterruptedException {
        try (OrientDB dbClient = new OrientDB("embedded:testDb", OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            Supplier<ODatabaseDocument> sessionSupplier = () -> dbClient.open(dbName, "admin", "admin");
            Repository repository = OrientDbRepository
                    .builder(sessionSupplier)
                    .scheduler(Schedulers.io())
                    .buildRepository(repositoryConfig);

            repository.entities(Product.metaClass).findAll().test().await();

            try (ODatabaseDocument session = sessionSupplier.get()) {
                OClass oClass = session.getClass("Product");
                oClass.createIndex("Product.fullIndex", "FULLTEXT", null, null, "LUCENE", new String[] {
                        "name",
                        "inventory.name"
                });
            }

            repository.entities(Product.metaClass).update(createProducts(30)).test().await().assertNoErrors();
            try (ODatabaseDocument session = sessionSupplier.get()) {
                session.query("select from Product where search_index('Product.fullIndex', '+Inventory +1') = true")
                        .stream()
                        .map(OResult::toString)
                        .forEach(System.out::println);
            }
        }
    }

    private Iterable<Product> createProducts(int count) {
        final Product.Type[] productTypes = {
                ProductPrototype.Type.ConsumerElectronics,
                ProductPrototype.Type.ComputeHardware,
                ProductPrototype.Type.ComputerSoftware
        };

        List<Inventory> inventories = IntStream.range(0, Math.max(1, count / 10))
                .mapToObj(i -> Inventory
                        .builder()
                        .id(UniqueId.inventoryId(i))
                        .name("Inventory " + i)
                        .build())
                .collect(Collectors.toList());

        List<Vendor> vendors = Stream
                .concat(
                        IntStream.range(0, 3)
                                .mapToObj(i -> Vendor
                                        .builder()
                                        .id(UniqueId.vendorId(i))
                                        .name("Vendor " + i)
                                        .build()),
                        Stream.of((Vendor)null))
                .collect(Collectors.toList());

        return IntStream.range(0, count)
                .mapToObj(i -> Product.builder()
                        .key(UniqueId.productId(i))
                        .name("Product " + i)
                        .type(productTypes[i % productTypes.length])
                        .inventory(inventories.get(i % inventories.size()))
                        .vendor(vendors.get(i % vendors.size()))
                        .price(100 + (i % 7)*(i % 11) + i % 13)
                        .build())
                .collect(Collectors.toList());
    }

    @Test
    public void testOrientDbSchemeProvider() throws InterruptedException {
        SchemaProvider schemaProvider = new OrientDbSchemaProvider(OrientDbSessionProvider.create(dbSessionSupplier), Schedulers.single());
        SchemaProvider cachedSchemaProvider = CacheSchemaProviderDecorator.decorate(schemaProvider);
        cachedSchemaProvider.createOrUpdate(Inventory.metaClass)
                .test()
                .await()
                .assertNoErrors()
                .assertComplete();
    }

    @Test @Ignore
    @UseLogLevel(LogLevel.TRACE)
    public void testObserveAsList() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(createProducts(10)).blockingGet();
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
                .assertValueAt(0, l -> l.get(0).name().equals("Product 2"))
                .assertValueAt(0, l -> l.get(2).name().equals("Product 4"));

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
                .blockingGet();

        productTestObserver
                .awaitCount(2, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 10000)
                .assertValueCount(2)
                .assertValueAt(1, l -> l.size() == 3)
                .assertValueAt(1, l -> l.get(0).name().equals("Product 2"))
                .assertValueAt(1, l -> l.get(1).name().equals("Product 3"))
                .assertValueAt(1, l -> l.get(2).name().equals("Product 3-1"));
    }

    @Test
    public void testOrientDbLiveQueryWithProjection() throws InterruptedException {
        repository.entities(Product.metaClass).update(createProducts(10))
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
                .assertValue(n -> "Product 1".equals(Objects.requireNonNull(n.oldValue()).name()))
                .assertValue(n -> "Product 1 - new name".equals(Objects.requireNonNull(n.newValue()).name()));

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
                .assertValue(n -> ProductPrototype.Type.ComputeHardware.equals(Objects.requireNonNull(n.oldValue()).type()))
                .assertValue(n -> ProductPrototype.Type.ComputerSoftware.equals(Objects.requireNonNull(n.newValue()).type()));
    }
}
