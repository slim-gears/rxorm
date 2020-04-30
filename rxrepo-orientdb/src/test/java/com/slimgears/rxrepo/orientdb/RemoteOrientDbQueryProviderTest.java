package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.util.SchedulingProvider;
import org.junit.Ignore;

@Ignore
public class RemoteOrientDbQueryProviderTest extends AbstractOrientDbQueryProviderTest {
    private static final String dbUrl = "remote:localhost/db";

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return super.createRepository(schedulingProvider, dbUrl, OrientDbRepository.Type.Memory);
    }
}
