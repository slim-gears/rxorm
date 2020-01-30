package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;

public class RecursiveLiveQueryProviderDecorator implements QueryProvider.Decorator {
    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    public static QueryProvider.Decorator create() {
        return new RecursiveLiveQueryProviderDecorator();
    }

    static class Decorator extends AbstractQueryProviderDecorator {
        protected Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query).compose(applyRecursiveObserve(query));
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
            return super.queryAndObserve(queryInfo, observeInfo).compose(applyRecursiveObserve(queryInfo));
        }

        private <K, S, T> ObservableTransformer<Notification<T>, Notification<T>> applyRecursiveObserve(QueryInfo<K, S, T> query) {
            return src -> src;
        }
    }
}
