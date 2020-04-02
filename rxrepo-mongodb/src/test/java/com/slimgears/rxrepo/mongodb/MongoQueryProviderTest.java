package com.slimgears.rxrepo.mongodb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.SubscribeOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import com.slimgears.util.test.logging.UseLogLevels;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

@UseLogLevels(
        @UseLogLevel(logger = "org.mongodb.driver", value = LogLevel.INFO)
)
@Ignore
public class MongoQueryProviderTest extends AbstractRepositoryTest {
    private static AutoCloseable mongoProcess;

    @BeforeClass
    public static void setUpClass() {
        mongoProcess = MongoTestUtils.startMongo();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (mongoProcess != null) {
            mongoProcess.close();
        }
    }

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return MongoRepository.builder()
                .port(MongoTestUtils.port)
                .maxConcurrentRequests(100)
                .decorate(SubscribeOnSchedulingQueryProviderDecorator.createDefault())
                .build();
    }
}
