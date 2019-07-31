package com.slimgears.rxrepo.mongodb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class MongoQueryProviderTest extends AbstractRepositoryTest {
    private static AutoCloseable mongoProcess;

    @BeforeClass
    public static void setUpClass() {
        mongoProcess = MongoTestUtils.startMongo();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        mongoProcess.close();
    }

    @Override
    protected Repository createRepository() {
        return MongoRepository.builder()
                .port(MongoTestUtils.port)
//                .user("root")
//                .password("example")
                .build();
    }
}
