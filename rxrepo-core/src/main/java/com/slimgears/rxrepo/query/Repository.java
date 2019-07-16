package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.function.Consumer;

public interface Repository extends AutoCloseable {
    <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta);

    void close();
    void clearAndClose();

    default Repository onClose(Consumer<Repository> onClose) {
        Repository self = this;

        return new Repository() {
            @Override
            public <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
                return self.entities(meta);
            }

            @Override
            public void clearAndClose() {
                onClose.accept(this);
                self.clearAndClose();
            }

            @Override
            public void close() {
                onClose.accept(this);
                self.close();
            }
        };
    }

    static Repository fromProvider(QueryProvider provider, QueryProvider.Decorator... decorators) {
        return fromProvider(provider, null, decorators);
    }

    static Repository fromProvider(QueryProvider provider, RepositoryConfigModel config, QueryProvider.Decorator... decorators) {
        QueryProvider.Decorator decorator = QueryProvider.Decorator.of(decorators);
        return new DefaultRepository(decorator.apply(provider), config);
    }
}
