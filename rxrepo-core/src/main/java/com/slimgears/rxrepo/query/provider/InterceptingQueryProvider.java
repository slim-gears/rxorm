package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.functions.Function;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class InterceptingQueryProvider implements QueryProvider, QueryPublisher {
    private final List<QueryPublisher.QueryListener> queryListeners = new CopyOnWriteArrayList<>();
    private final QueryProvider underlyingProvider;

    private InterceptingQueryProvider(QueryProvider underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
    }

    public static InterceptingQueryProvider of(QueryProvider provider) {
        return new InterceptingQueryProvider(provider);
    }

    @Override
    public Disposable subscribe(QueryPublisher.QueryListener queryListener) {
        queryListeners.add(queryListener);
        return Disposables.fromAction(() -> queryListeners.remove(queryListener));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        return underlyingProvider.insertOrUpdate(entity);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return underlyingProvider.insertOrUpdate(metaClass, key, entityUpdater);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return underlyingProvider
                .query(query)
                .compose(applyOnQuery(query));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return underlyingProvider
                .liveQuery(query)
                .compose(applyOnLiveQuery(query));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return underlyingProvider.aggregate(query, aggregator);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update) {
        return underlyingProvider.update(update);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return underlyingProvider.delete(delete);
    }

    private <K, S extends HasMetaClassWithKey<K, S>, T> ObservableTransformer<T, T> applyOnQuery(QueryInfo<K, S, T> queryInfo) {
        return source -> {
            AtomicReference<Observable<T>> observable = new AtomicReference<>(source);
            queryListeners.forEach(l -> observable.updateAndGet(o -> l.onQuery(queryInfo, o)));
            return observable.get();
        };
    }

    private <K, S extends HasMetaClassWithKey<K, S>, T> ObservableTransformer<Notification<T>, Notification<T>> applyOnLiveQuery(QueryInfo<K, S, T> queryInfo) {
        return source -> {
            AtomicReference<Observable<Notification<T>>> observable = new AtomicReference<>(source);
            queryListeners.forEach(l -> observable.updateAndGet(o -> l.onLiveQuery(queryInfo, o)));
            return observable.get();
        };
    }
}
