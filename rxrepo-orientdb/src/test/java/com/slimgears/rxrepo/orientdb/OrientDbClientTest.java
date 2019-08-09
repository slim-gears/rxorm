package com.slimgears.rxrepo.orientdb;

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
import com.slimgears.rxrepo.sql.CacheSchemaProviderDecorator;
import com.slimgears.rxrepo.sql.SchemaProvider;
import com.slimgears.rxrepo.test.Inventory;
import com.slimgears.rxrepo.test.UniqueId;
import io.reactivex.functions.Consumer;
import org.junit.Ignore;
import org.junit.Test;

import java.util.function.Supplier;

public class OrientDbClientTest {
    private static final String dbUrl = "embedded:db";
    private final static String dbName = "testDb";

    @Test
    public void orientDbClientTest() {
        OrientDB client = createClient(dbName);
        client.close();

        client = createClient(dbName);
        client.close();
    }

    private OrientDB createClient(String dbName) {
        OrientDB db = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        db.createIfNotExists(dbName, ODatabaseType.PLOCAL);
        ODatabaseDocument session = db.open(dbName, "admin", "admin");
        session.createClassIfNotExist("MyClass");
        session.command("insert into MyClass set `name`=?", "test")
                .stream()
                .forEach(System.out::println);

        session.command("select count() from MyClass")
                .stream()
                .forEach(System.out::println);

        session.close();
        return db;
    }

    private void rawDbTest(Consumer<ODatabaseSession> test) throws Exception {
        try (OrientDB dbClient = new OrientDB(dbUrl, OrientDBConfig.defaultConfig())) {
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

    @Test
    public void testOrientDbSchemeProvider() throws InterruptedException {
        OrientDB dbClient = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        dbClient.createIfNotExists(dbName, ODatabaseType.MEMORY);
        try {
            Supplier<ODatabaseDocument> dbSessionSupplier = () -> dbClient.open(dbName, "admin", "admin");
            SchemaProvider schemaProvider = new OrientDbSchemaProvider(OrientDbSessionProvider.create(dbSessionSupplier));
            SchemaProvider cachedSchemaProvider = CacheSchemaProviderDecorator.decorate(schemaProvider);
            cachedSchemaProvider.createOrUpdate(Inventory.metaClass)
                    .test()
                    .await()
                    .assertNoErrors()
                    .assertComplete();
        } finally {
            dbClient.drop(dbName);
        }
    }
}
