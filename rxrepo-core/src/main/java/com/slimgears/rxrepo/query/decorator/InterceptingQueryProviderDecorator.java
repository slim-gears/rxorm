package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.QueryPublisher;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class InterceptingQueryProviderDecorator implements QueryProvider.Decorator, QueryPublisher {
    private final List<QueryPublisher.QueryListener> queryListeners = new CopyOnWriteArrayList<>();

    @Override
    public Disposable subscribe(QueryPublisher.QueryListener queryListener) {
        queryListeners.add(queryListener);
        return Disposables.fromAction(() -> queryListeners.remove(queryListener));
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    private class Decorator extends AbstractQueryProviderDecorator {
        private Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
            return super.query(query).compose(applyOnQuery(query));
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query).compose(applyOnLiveQuery(query));
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> query) {
            return super.queryAndObserve(query).compose(applyOnLiveQuery(query));
        }

        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
            return super.queryAndObserve(queryInfo, observeInfo).compose(applyOnLiveQuery(queryInfo));
        }

        private <K, S, T> ObservableTransformer<T, T> applyOnQuery(QueryInfo<K, S, T> queryInfo) {
            return source -> {
                AtomicReference<Observable<T>> observable = new AtomicReference<>(source);
                queryListeners.forEach(l -> observable.updateAndGet(o -> l.onQuery(queryInfo, o)));
                return observable.get();
            };
        }

        private <K, S, T> ObservableTransformer<Notification<T>, Notification<T>> applyOnLiveQuery(QueryInfo<K, S, T> queryInfo) {
            return source -> {
                AtomicReference<Observable<Notification<T>>> observable = new AtomicReference<>(source);
                queryListeners.forEach(l -> observable.updateAndGet(o -> l.onLiveQuery(queryInfo, o)));
                return observable.get();
            };
        }
    }
}
