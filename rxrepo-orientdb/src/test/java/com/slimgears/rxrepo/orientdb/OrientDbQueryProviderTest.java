package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.util.SchedulingProvider;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Test
    public void testObserveOn() {
        Logger log = LoggerFactory.getLogger(this.getClass());
        Completable action = Completable.fromAction(() -> log.info("In action"))
                .subscribeOn(Schedulers.single())
                .cache();
        action.blockingAwait();
        Flowable.range(0, 100)
                .buffer(10)
                .parallel()
                .runOn(Schedulers.io())
                .flatMap(i -> action.andThen(Completable
                        .fromAction(() -> log.info("Current batch: {}", i))
                        .toFlowable()))
                .sequential()
                .ignoreElements()
                .blockingAwait();
    }
}
