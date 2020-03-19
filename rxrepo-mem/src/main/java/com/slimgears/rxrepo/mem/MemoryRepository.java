package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.LiveQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.SubscribeOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;

public class MemoryRepository {
    public static Repository create(QueryProvider.Decorator... decorators) {
        return Repository
                .fromProvider(
                        new MemoryQueryProvider(),
                        SubscribeOnSchedulingQueryProviderDecorator.createDefault(),
                        LiveQueryProviderDecorator.create(),
                        UpdateReferencesFirstQueryProviderDecorator.create(),
                        QueryProvider.Decorator.of(decorators));
    }
}
