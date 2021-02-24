package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.postgres.PostgresRepository;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.rxrepo.util.SchedulingProvider;
import javafx.geometry.Pos;
import org.junit.BeforeClass;

public class PostgresRepositoryTest  extends AbstractRepositoryTest {
    @BeforeClass
    public static void setUpClass() {
        PostgresTestUtils.start();
    }

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return PostgresRepository
                .builder()
                .connection(PostgresTestUtils.connectionUrl)
                .schemaName(PostgresTestUtils.schemaName)
                .enableBatch(1000)
                .schedulingProvider(schedulingProvider)
                .build();
    }
}
