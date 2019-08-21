package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Observable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
public class CacheQueryProviderDecorator implements QueryProvider.Decorator {
    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new DecoratedProvider(queryProvider);
    }

    public static QueryProvider.Decorator create() {
        return new CacheQueryProviderDecorator();
    }

    private static class DecoratedProvider extends AbstractQueryProviderDecorator {
        private final Map<QueryInfo<?, ?, ?>, Observable<Notification<?>>> activeQueries = new ConcurrentHashMap<>();

        private DecoratedProvider(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return (Observable<Notification<T>>)(Observable<?>)activeQueries
                    .computeIfAbsent(query, q -> (Observable<Notification<?>>)(Observable<?>)super
                            .liveQuery(query)
                            .doOnTerminate(() -> activeQueries.remove(query))
                            .share());
        }
    }
}
