package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.DelegateExpression;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Completable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public interface Repository extends AutoCloseable {
    <K, T> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta);

    default <K, T> EntitySet<K, T> entities(DelegateExpression<T, T> expression) {
        return entities(MetaClasses.forTokenWithKeyUnchecked(expression.reflect().objectType()));
    }

    Iterable<EntitySet<?, ?>> allEntitySets();

    void close();
    Completable clear();

    default Repository onClose(Consumer<Repository> onClose) {
        Repository self = this;
        AtomicBoolean closed = new AtomicBoolean();

        return new Repository() {
            @Override
            public <K, T> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
                return self.entities(meta);
            }

            @Override
            public Iterable<EntitySet<?, ?>> allEntitySets() {
                return self.allEntitySets();
            }

            @Override
            public Completable clear() {
                return self.clear();
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    onClose.accept(this);
                    self.close();
                }
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
