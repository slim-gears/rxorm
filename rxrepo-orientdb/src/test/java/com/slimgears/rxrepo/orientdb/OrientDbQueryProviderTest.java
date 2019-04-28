package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.EntitySet;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.util.test.AnnotationRulesJUnit;
import com.slimgears.util.test.UseLogLevel;
import io.reactivex.Maybe;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.CompletableSubject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
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

@RunWith(AnnotationRulesJUnit.class)
//@UseLogLevel(UseLogLevel.Level.FINE)
public class OrientDbQueryProviderTest {
    @Rule public final TestName testNameRule = new TestName();
    private static final String dbName = "testDb";
    private static OServer server;
    private Repository repository;
    private OrientDB dbClient;

    @BeforeClass
    public static void setUpClass() throws Exception {
        server = OServerMain.create(true);
        server.startup(new OServerConfiguration());
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
        repository = OrientDbRepository
                .builder(() -> dbClient.open(dbName, "admin", "admin"))
                .scheduler(Schedulers.io())
                .buildRepository();
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
                .observe()
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
    //@UseLogLevel(UseLogLevel.Level.FINEST)
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
    //@UseLogLevel(UseLogLevel.Level.FINEST)
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
                .observe()
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
    @UseLogLevel(UseLogLevel.Level.FINEST)
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
    @UseLogLevel(UseLogLevel.Level.FINEST)
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
                    return matcher.matches() && matcher.group(1).equals(matcher.group(2));
                });
    }

    @Test
    @UseLogLevel(UseLogLevel.Level.FINEST)
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
                .assertValueAt(1, p -> "Product 1".equals(p.name()));
    }

    @Test @Ignore
    public void testAddThenQueryRawOrientDb() {
        try (OrientDB dbClient = new OrientDB("embedded:testDbServer", OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            try (ODatabaseSession dbSession = dbClient.open(dbName, "admin", "admin")) {
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
            }
        }
    }

    @Test @Ignore
    public void testCustomIndex() {
        try (OrientDB dbClient = new OrientDB("embedded:testDb", OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            try (ODatabaseSession session = dbClient.open(dbName, "admin", "admin")) {
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
            }
        }
    }

    @Test @Ignore
    @UseLogLevel(UseLogLevel.Level.FINEST)
    public void testRawOrientDbLuceneIndex() throws InterruptedException {
        try (OrientDB dbClient = new OrientDB("embedded:testDb", OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            Supplier<ODatabaseDocument> sessionSupplier = () -> dbClient.open(dbName, "admin", "admin");
            Repository repository = OrientDbRepository
                    .builder(sessionSupplier)
                    .scheduler(Schedulers.io())
                    .buildRepository();

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

        return IntStream.range(0, count)
                .mapToObj(i -> Product.builder()
                        .key(UniqueId.productId(i))
                        .name("Product " + i)
                        .type(productTypes[i % productTypes.length])
                        .inventory(inventories.get(i % inventories.size()))
                        .price(100 + (i % 7)*(i % 11) + i % 13)
                        .build())
                .collect(Collectors.toList());
    }
}
