package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public class CacheQueryProviderDecorator implements UnaryOperator<QueryProvider> {
    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new DecoratedProvider(queryProvider);
    }

    public static UnaryOperator<QueryProvider> create() {
        return new CacheQueryProviderDecorator();
    }

    private static class DecoratedProvider extends AbstractQueryProviderDecorator {
        private final Map<QueryInfo, Observable<Notification<?>>> activeQueries = new ConcurrentHashMap<>();

        private DecoratedProvider(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return (Observable<Notification<T>>)(Observable)activeQueries
                    .computeIfAbsent(query, q -> (Observable<Notification<?>>)(Observable)super
                            .liveQuery(query)
                            .doOnTerminate(() -> activeQueries.remove(query))
                            .share());
        }
    }
}
