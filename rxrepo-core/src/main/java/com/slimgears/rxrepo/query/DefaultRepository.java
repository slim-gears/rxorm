package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.decorator.MandatoryPropertiesQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultRepository implements Repository {
    private final static RepositoryConfigModel defaultConfig = RepositoryConfig
            .builder()
            .retryCount(10)
            .debounceTimeoutMillis(100)
            .retryInitialDurationMillis(10)
            .build();

    private final RepositoryConfigModel config;
    private final QueryProvider queryProvider;
    private final Map<MetaClassWithKey<?, ?>, EntitySet<?, ?>> entitySetMap = new HashMap<>();

    DefaultRepository(QueryProvider queryProvider, RepositoryConfigModel config) {
        this.queryProvider = QueryProvider.Decorator.of(
                MandatoryPropertiesQueryProviderDecorator.create())
                .apply(queryProvider);
        this.config = Optional.ofNullable(config).orElse(defaultConfig);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, T> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
        return (EntitySet<K, T>)entitySetMap.computeIfAbsent(meta, m -> createEntitySet(meta));
    }

    @Override
    public void clearAndClose() {
        clear();
        close();
    }

    private <K, T> EntitySet<K, T> createEntitySet(MetaClassWithKey<K, T> metaClass) {
        return DefaultEntitySet.create(queryProvider, metaClass, config);
    }

    @Override
    public void close() {
        this.queryProvider.close();
    }

    @Override
    public void clear() {
        queryProvider.dropAll().blockingAwait();
    }
}
