package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.util.SchedulingProvider;
import org.junit.Ignore;


public class RemoteOrientDbQueryProviderTest extends AbstractOrientDbQueryProviderTest {
    //replace the URL with the relevant one
//    private static final String dbUrl = "remote:10.55.136.177/db;remote:10.55.136.177:2425/db";
    private static final String dbUrl = "remote:localhost/db;remote:localhost:2425/db";

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        return super.createRepository(schedulingProvider, dbUrl, OrientDbRepository.Type.Persistent);
//        return super.createRepository(schedulingProvider, dbUrl, OrientDbRepository.Type.Memory);
    }
}
