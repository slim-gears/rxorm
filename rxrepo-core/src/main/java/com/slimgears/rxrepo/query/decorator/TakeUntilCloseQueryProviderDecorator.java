package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.subjects.CompletableSubject;

import java.util.concurrent.atomic.AtomicBoolean;

public class TakeUntilCloseQueryProviderDecorator implements QueryProvider.Decorator {
    private final static Object onCloseToken = new Object();

    private TakeUntilCloseQueryProviderDecorator() {
    }

    public static QueryProvider.Decorator create() {
        return new TakeUntilCloseQueryProviderDecorator();
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    static class Decorator extends AbstractQueryProviderDecorator {
        private final CompletableSubject closeSubject = CompletableSubject.create();
        private final AtomicBoolean wasClosed = new AtomicBoolean();

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

        @Override
        public void close() {
            if (wasClosed.compareAndSet(false, true)) {
                closeSubject.onComplete();
                super.close();
            }
        }

        @Override
        protected QueryProvider getUnderlyingProvider() {
            return wasClosed.get() ? EmptyQueryProvider.instance : super.getUnderlyingProvider();
        }

        private <T> ObservableTransformer<T, T> applyTakeUntilClose() {
            return src -> src.takeUntil(closeSubject.andThen(Observable.just(onCloseToken)));
        }
    }
}
