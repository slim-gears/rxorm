package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OrientDbQueryProviderTest extends AbstractRepositoryTest {
    private static final String dbUrl = "embedded:testDb";
    private static final String dbName = "testDb";

    @Parameterized.Parameter public OrientDbRepository.Type dbType;

    @Parameterized.Parameters
    public static OrientDbRepository.Type[] params() {
        return new OrientDbRepository.Type[] {OrientDbRepository.Type.Memory, OrientDbRepository.Type.Persistent};
    }

    @Override
    protected Repository createRepository() {
        return OrientDbRepository
                .builder()
                .url(dbUrl)
                .debounceTimeoutMillis(1000)
                .type(dbType)
                .name(dbName)
                .build();
    }

    @Test
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }
}
