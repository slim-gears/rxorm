package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.SchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.util.generic.MoreStrings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OrientDbQueryProviderTest extends AbstractRepositoryTest {
    private static final String dbUrl = "embedded:db";
    private static final String dbName = "{}_{}";

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
                .build();
    }

    @Test
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }
}
