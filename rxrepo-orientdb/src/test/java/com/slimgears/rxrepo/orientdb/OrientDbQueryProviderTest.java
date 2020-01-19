package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.SchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.util.generic.MoreStrings;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OrientDbQueryProviderTest extends AbstractRepositoryTest {
    private static final String dbUrl = "embedded:db";
    private static final String dbName = "{}_{}";
    private static LoggingMeterRegistry loggingMeterRegistry;

    @BeforeClass
    public static void setUpClass() {
        loggingMeterRegistry = new LoggingMeterRegistry();
        SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry
                .add(loggingMeterRegistry)
                .add(simpleMeterRegistry);
    }

    @AfterClass
    public static void tearDownClass() {
        loggingMeterRegistry.stop();
        Metrics.globalRegistry.close();
    }

    @Parameterized.Parameter public OrientDbRepository.Type dbType;

    @Parameterized.Parameters
    public static OrientDbRepository.Type[] params() {
        return new OrientDbRepository.Type[] {
                OrientDbRepository.Type.Memory,
                OrientDbRepository.Type.Persistent};
    }

    @Override
    protected Repository createRepository() {
        String name = MoreStrings.format(dbName, dbType, testNameRule.getMethodName().replaceAll("\\[\\d+]", ""));
        return OrientDbRepository
                .builder()
                .url(dbUrl)
                .debounceTimeoutMillis(1000)
                .type(dbType)
                .name(name)
                .decorate(SchedulingQueryProviderDecorator.createDefault())
                .enableBatchSupport()
                .build();
    }

    @Test
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }
}
