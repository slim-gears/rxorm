package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.EntitySet;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.sql.DefaultSqlStatementProvider;
import com.slimgears.rxrepo.sql.SqlServiceFactory;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
        OrientDbSessionProvider sessionProvider = new OrientDbSessionProvider(() -> dbClient.open(dbName, "admin", "admin"));

        QueryProvider queryProvider = SqlServiceFactory.builder()
                .schemaProvider(() -> new OrientDbSchemaProvider(sessionProvider))
                .statementExecutor(() -> new OrientDbStatementExecutor(sessionProvider))
                .expressionGenerator(OrientDbSqlExpressionGenerator::new)
                .statementProvider(svc -> new DefaultSqlStatementProvider(svc.expressionGenerator(), svc.schemaProvider()))
                .referenceResolver(svc -> new OrientDbReferenceResolver(svc.statementProvider()))
                .build()
                .queryProvider();

        repository = Repository.fromProvider(queryProvider);
    }

    @After
    public void tearDown() {
        dbClient.drop(dbName);
        dbClient.close();
    }

    @Test
    public void testInsertLiveRetrieve() throws InterruptedException {
        EntitySet<ProductKey, Product> productSet = repository.entities(Product.metaClass);

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
                .awaitCount(1029)
                .assertValueAt(1028, Notification::isDelete)
                .assertNoErrors();

        productCount
                .awaitCount(1029)
                .assertNoErrors();
    }

    @Test
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<ProductKey, Product> productSet = repository.entities(Product.metaClass);
        productSet
                .update(Arrays.asList(
                        Product.builder()
                                .name("Product 1")
                                .key(ProductKey.create(1))
                                .price(1001)
                                .inventory(Inventory
                                        .builder()
                                        .id(2)
                                        .name("Inventory 2")
                                        .build())
                                .build(),
                        Product.builder()
                                .name("Product 2")
                                .key(ProductKey.create(2))
                                .price(1002)
                                .inventory(Inventory
                                        .builder()
                                        .id(2)
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
    }

    @Test
    public void testInsertThenRetrieve() throws InterruptedException {
        EntitySet<ProductKey, Product> productSet = repository.entities(Product.metaClass);
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

        //noinspection unchecked
        productSet
                .query()
                .where(Product.$.name.contains("231"))
                .select()
                .retrieve(Product.$.key.id, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValue(p -> p.name() == null)
                .assertValue(p -> p.key().id() == 231)
                .assertValue(p -> p.price() == 110)
                .assertValue(p -> "Inventory 31".equals(Objects.requireNonNull(p.inventory()).name()))
                .assertValueCount(1);
    }

    @Test
    @UseLogLevel(UseLogLevel.Level.FINEST)
    public void testInsertThenSearch() throws InterruptedException {
        EntitySet<ProductKey, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = createProducts(100);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        //noinspection unchecked
        productSet
                .query()
                .where(Product.$.searchText("Product 31"))
                .select()
                .retrieve(Product.$.key.id, Product.$.name, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    public void testInsertThenUpdate() throws InterruptedException {
        EntitySet<ProductKey, Product> productSet = repository.entities(Product.metaClass);
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
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(20)
                .assertValueAt(15, pr -> {
                    Matcher matcher = Pattern.compile("Product 1([0-9]+) - Inventory ([0-9]+)").matcher(Objects.requireNonNull(pr.name()));
                    return matcher.matches() && matcher.group(1).equals(matcher.group(2));
                });
    }

    private Iterable<Product> createProducts(int count) {
        List<Inventory> inventories = IntStream.range(0, count / 10)
                .mapToObj(i -> Inventory.builder().id(i).name("Inventory " + i).build())
                .collect(Collectors.toList());

        return IntStream.range(0, count)
                .mapToObj(i -> Product.builder()
                        .key(ProductKey.create(i))
                        .name("Product " + i)
                        .inventory(inventories.get(i % inventories.size()))
                        .price(100 + (i % 7)*(i % 11) + i % 13)
                        .build())
                .collect(Collectors.toList());
    }
}
