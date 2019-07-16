package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Test;

public class OrientDbClientTest {
    @Test
    public void orientDbClientTest() {
        OrientDB client = createClient("testDb");
        client.close();

        client = createClient("testDb");
        client.close();
    }

    private OrientDB createClient(String dbName) {
        OrientDB db = new OrientDB("embedded:test", OrientDBConfig.defaultConfig());
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
}
