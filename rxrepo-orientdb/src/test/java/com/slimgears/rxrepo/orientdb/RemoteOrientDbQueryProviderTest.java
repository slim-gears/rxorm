package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.DockerComposeRule;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.List;

public class RemoteOrientDbQueryProviderTest extends OrientDbQueryProviderTest {
    private static final String dbUrl = "remote:localhost/db";

    @ClassRule public static final TestRule orientDbContainerRule = new DockerComposeRule();

    @Override
    protected Repository createRepository() {
        return createRepository(OrientDbRepository.Type.Memory);
    }

    @BeforeClass
    public static void setUpClass() {
        Assume.assumeTrue(isDbAvailable());
    }

    private static boolean isDbAvailable() {
        try {

            OrientDB client = new OrientDB(dbUrl, "root", "root", OrientDBConfig.defaultConfig());
            List<String> dbs = client.list();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected Repository createRepository(OrientDbRepository.Type dbType) {
        return super.createRepository(dbUrl, dbType);
    }
}
