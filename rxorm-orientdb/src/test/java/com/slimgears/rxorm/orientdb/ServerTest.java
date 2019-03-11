package com.slimgears.rxorm.orientdb;

import com.google.common.base.Stopwatch;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;
import com.slimgears.util.repository.query.DefaultRepository;
import com.slimgears.util.repository.query.EntitySet;
import com.slimgears.util.repository.query.Repository;
import org.junit.Test;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServerTest {
    private static final String dbName = "testDb";

    @Test
    public void serverTest() throws Exception {
        OServer server = OServerMain.create(true);
        server.startup(new OServerConfiguration());
        OrientDB dbClient = new OrientDB("embedded:testDbServer", OrientDBConfig.defaultConfig());
        dbClient.create(dbName, ODatabaseType.MEMORY);
        Provider<ODatabaseSession> sessionProvider = () -> dbClient.open(dbName, "admin", "admin");

        OrientDbQueryProvider queryProvider = new OrientDbQueryProvider(sessionProvider);

        List<Inventory> inventories = IntStream.range(0, 100)
                .mapToObj(i -> Inventory.builder().id(i).name("Inventory " + i).build())
                .collect(Collectors.toList());

        List<Product> products = IntStream.range(0, 1000)
                .mapToObj(i -> Product.builder().id(i).name("Product " + i).inventory(inventories.get(i % inventories.size())).build())
                .collect(Collectors.toList());

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        Repository repository = new DefaultRepository(queryProvider);
        EntitySet<Long, Product, Product.Builder> productSet = repository.entities(Product.metaClass);
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        System.out.println("Duration: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
//        productSet.query().select().count()
//                .test()
//                .await()
//                .assertValue(1L);

//        productSet
//                .query()
//                .where(Product.$.name.contains("231"))
//                .select()
//                .retrieve()
//                .test()
//                .await()
//                .assertValueCount(1);
    }

    @SafeVarargs
    private static <T> T configure(T instance, Consumer<T>... configs) {
        Arrays.asList(configs).forEach(c -> c.accept(instance));
        return instance;
    }

    private static OServerParameterConfiguration[] params(OServerParameterConfiguration... params) {
        return params;
    }

    private static OServerParameterConfiguration param(String name, String value) {
        return configure(new OServerParameterConfiguration(), c -> c.name = name, c -> c.value = value);
    }
}
