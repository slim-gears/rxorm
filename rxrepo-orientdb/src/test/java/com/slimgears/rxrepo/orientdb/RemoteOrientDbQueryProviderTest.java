package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.DockerUtils;
import com.slimgears.rxrepo.util.SchedulingProvider;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.util.List;

public class RemoteOrientDbQueryProviderTest extends OrientDbQueryProviderTest {
    private static final String dbUrl = "remote:localhost/db";

    @BeforeClass
    public static void setUpClass() {
        DockerUtils.start();
        Assume.assumeTrue(isDbAvailable(dbUrl));
    }

    @AfterClass
    public static void tearDownClass() {
        DockerUtils.stop();
    }

    private static boolean isDbAvailable(String dbUrl) {
        try {
            OrientDB client = new OrientDB(dbUrl, "root", "root", OrientDBConfig.defaultConfig());
            List<String> dbs = client.list();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider, OrientDbRepository.Type dbType) {
        return super.createRepository(schedulingProvider, dbUrl, dbType);
    }
}
