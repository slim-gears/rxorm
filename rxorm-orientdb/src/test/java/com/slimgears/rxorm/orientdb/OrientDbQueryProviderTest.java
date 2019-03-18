package com.slimgears.rxorm.orientdb;

import com.google.common.base.Stopwatch;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.slimgears.util.repository.expressions.Aggregator;
import com.slimgears.util.repository.query.EntitySet;
import com.slimgears.util.repository.query.Notification;
import com.slimgears.util.repository.query.Repository;
import com.slimgears.util.test.AnnotationRulesJUnit;
import com.slimgears.util.test.UseLogLevel;
import io.reactivex.observers.TestObserver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(AnnotationRulesJUnit.class)
@UseLogLevel(UseLogLevel.Level.FINE)
public class OrientDbQueryProviderTest {
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
        dbClient = new OrientDB("embedded:testDbServer", OrientDBConfig.defaultConfig());
        dbClient.create(dbName, ODatabaseType.MEMORY);
        Provider<ODatabaseSession> sessionProvider = () -> dbClient.open(dbName, "admin", "admin");
        OrientDbQueryProvider queryProvider = new OrientDbQueryProvider(sessionProvider);
        repository = Repository.fromProvider(queryProvider);
    }

    @After
    public void tearDown() {
        dbClient.drop(dbName);
        dbClient.close();
    }

    @Test
    public void testInsertLiveRetrieve() throws InterruptedException {
        EntitySet<Integer, Product> productSet = repository.entities(Product.metaClass);

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
                .await();

        productUpdatesTest
                .awaitCount(1000);

        productSet.delete().where(Product.$.id.betweenExclusive(100, 130))
                .execute()
                .subscribe();

        productUpdatesTest
                .awaitCount(1029)
                .assertValueAt(1028, Notification::isDelete);

        productCount
                .awaitCount(1029);
    }

    @Test
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<Integer, Product> productSet = repository.entities(Product.metaClass);
        productSet
                .update(Arrays.asList(
                        Product.builder()
                                .name("Product 1")
                                .id(1)
                                .price(1001)
                                .inventory(Inventory
                                        .builder()
                                        .id(1)
                                        .name("Inventory 1")
                                        .build())
                                .build(),
                        Product.builder()
                                .name("Product 2")
                                .id(2)
                                .price(1002)
                                .inventory(Inventory
                                        .builder()
                                        .id(1)
                                        .name("Inventory 1")
                                        .build())
                                .build()
                ))
                .test()
                .await();
    }

    @Test
    public void testInsertThenRetrieve() throws InterruptedException {
        EntitySet<Integer, Product> productSet = repository.entities(Product.metaClass);
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
                .retrieve(Product.$.id, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertValue(p -> p.name() == null)
                .assertValue(p -> p.id() == 231)
                .assertValue(p -> p.price() == 110)
                .assertValue(p -> "Inventory 31".equals(p.inventory().name()))
                .assertValueCount(1);
    }

    private Iterable<Product> createProducts(int count) {
        List<Inventory> inventories = IntStream.range(0, count / 10)
                .mapToObj(i -> Inventory.builder().id(i).name("Inventory " + i).build())
                .collect(Collectors.toList());

        return IntStream.range(0, count)
                .mapToObj(i -> Product.builder()
                        .id(i)
                        .name("Product " + i)
                        .inventory(inventories.get(i % inventories.size()))
                        .price(100 + (i % 7)*(i % 11) + i % 13)
                        .build())
                .collect(Collectors.toList());
    }
}
