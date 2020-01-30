package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;

public class TakeUntilCloseQueryProviderDecorator implements QueryProvider.Decorator {
    private final static Object onCloseToken = new Object();
    private final Completable closeCompletable;

    private TakeUntilCloseQueryProviderDecorator(Completable closeCompletable) {
        this.closeCompletable = closeCompletable;
    }

    public static QueryProvider.Decorator create(Completable closeCompletable) {
        return new TakeUntilCloseQueryProviderDecorator(closeCompletable);
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    class Decorator extends AbstractQueryProviderDecorator {
        private Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query).compose(applyTakeUntilClose());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
            return super.queryAndObserve(queryInfo, observeInfo).compose(applyTakeUntilClose());
        }

        @Override
        public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.liveAggregate(query, aggregator).compose(applyTakeUntilClose());
        }

        private <T> ObservableTransformer<T, T> applyTakeUntilClose() {
            return src -> src.takeUntil(closeCompletable.andThen(Observable.just(onCloseToken)));
        }
    }
}
