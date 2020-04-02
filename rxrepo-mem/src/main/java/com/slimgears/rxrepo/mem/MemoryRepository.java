package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.LiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.ObserveOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.SubscribeOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.CachedRoundRobinSchedulingProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;

import java.time.Duration;

public class MemoryRepository {
    public static Repository create(QueryProvider.Decorator... decorators) {
        SchedulingProvider schedulingProvider = CachedRoundRobinSchedulingProvider.create(10, Duration.ofMinutes(1));
        return Repository
                .fromProvider(
                        MemoryQueryProvider.create(schedulingProvider),
                        LiveQueryProviderDecorator.create(),
                        ObserveOnSchedulingQueryProviderDecorator.create(schedulingProvider),
                        SubscribeOnSchedulingQueryProviderDecorator.createDefault(),
                        UpdateReferencesFirstQueryProviderDecorator.create(),
                        QueryProvider.Decorator.of(decorators));
    }
}
