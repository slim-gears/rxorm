package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.util.SchedulingProvider;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OrientDbQueryProviderTest extends AbstractOrientDbQueryProviderTest {
    private static final String dbUrl = "embedded:db";

    @Parameterized.Parameter public OrientDbRepository.Type dbType;

    @Parameterized.Parameters
    public static OrientDbRepository.Type[] params() {
        return new OrientDbRepository.Type[] {
                OrientDbRepository.Type.Memory,
                OrientDbRepository.Type.Persistent};
    }

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return createRepository(schedulingProvider, dbType);
    }

    protected Repository createRepository(SchedulingProvider schedulingProvider, OrientDbRepository.Type dbType) {
        return super.createRepository(schedulingProvider, dbUrl, dbType);
    }
}
