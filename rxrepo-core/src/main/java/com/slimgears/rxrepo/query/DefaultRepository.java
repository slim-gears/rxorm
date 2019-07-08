package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultRepository implements Repository {
    private final static RepositoryConfiguration defaultConfig = new RepositoryConfiguration() {
        @Override
        public int retryCount() {
            return 10;
        }

        @Override
        public int debounceTimeoutMillis() {
            return 100;
        }

        @Override
        public int retryInitialDurationMillis() {
            return 10;
        }
    };

    private final RepositoryConfiguration config;
    private final QueryProvider queryProvider;
    private final Map<MetaClassWithKey<?, ?>, EntitySet<?, ?>> entitySetMap = new HashMap<>();

    DefaultRepository(QueryProvider queryProvider, RepositoryConfiguration config) {
        this.queryProvider = queryProvider;
        this.config = Optional.ofNullable(config).orElse(defaultConfig);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
        return (EntitySet<K, T>)entitySetMap.computeIfAbsent(meta, m -> createEntitySet(meta));
    }

    private <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> createEntitySet(MetaClassWithKey<K, T> metaClass) {
        return DefaultEntitySet.create(queryProvider, metaClass, config);
    }
}
