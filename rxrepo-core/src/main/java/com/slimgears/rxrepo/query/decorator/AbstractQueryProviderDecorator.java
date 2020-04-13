package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static com.slimgears.util.generic.LazyString.lazy;

public class AbstractQueryProviderDecorator implements QueryProvider {
    private final QueryProvider underlyingProvider;
    protected final Logger log;

    protected AbstractQueryProviderDecorator(QueryProvider underlyingProvider) {
        this.log = LoggerFactory.getLogger(getClass());
        this.underlyingProvider = underlyingProvider;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return getUnderlyingProvider().insert(metaClass, entities, recursive);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return getUnderlyingProvider().insertOrUpdate(metaClass, entity, recursive);
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return getUnderlyingProvider().insertOrUpdate(metaClass, key, recursive, entityUpdater);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        return getUnderlyingProvider().query(query);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return getUnderlyingProvider().queryAndObserve(queryInfo, observeInfo)
                .doOnNext(n -> log.trace("{}", Notifications.toBriefString(queryInfo.metaClass(), n)));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return getUnderlyingProvider().liveQuery(query)
                .doOnNext(n -> log.trace("{}", Notifications.toBriefString(query.metaClass(), n)));
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return getUnderlyingProvider().aggregate(query, aggregator);
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return getUnderlyingProvider().liveAggregate(query, aggregator)
            .doOnNext(n -> log.trace("[{}] Received aggregation notification: {}", lazy(() -> getClass().getSimpleName()), n));
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return getUnderlyingProvider().update(update);
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return getUnderlyingProvider().delete(delete);
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return getUnderlyingProvider().drop(metaClass);
    }

    @Override
    public Completable dropAll() {
        return getUnderlyingProvider().dropAll();
    }

    @Override
    public void close() {
        getUnderlyingProvider().close();
    }

    protected QueryProvider getUnderlyingProvider() {
        return underlyingProvider;
    }
}
