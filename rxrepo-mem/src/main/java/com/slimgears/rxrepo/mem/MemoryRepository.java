package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.*;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.CachedRoundRobinSchedulingProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.rxrepo.util.SemaphoreLockProvider;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import java.time.Duration;

public class MemoryRepository {
    public static Repository create(QueryProvider.Decorator... decorators) {
        SchedulingProvider schedulingProvider = CachedRoundRobinSchedulingProvider.create(10, Duration.ofMinutes(1));
        return Repository
                .fromProvider(
                        MemoryQueryProvider.create(schedulingProvider),
                        RetryOnConcurrentConflictQueryProviderDecorator.create(Duration.ofMillis(1), 5),
                        //LockQueryProviderDecorator.create(SemaphoreLockProvider.create()),
                        LiveQueryProviderDecorator.create(Duration.ofMillis(2000)),
                        ObserveOnSchedulingQueryProviderDecorator.create(schedulingProvider),
                        SubscribeOnSchedulingQueryProviderDecorator.createDefault(),
                        UpdateReferencesFirstQueryProviderDecorator.create(),
                        QueryProvider.Decorator.of(decorators));
    }
}
