package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.function.UnaryOperator;

public class OrientDbQueryProviderDecorator implements QueryProvider {
    private final QueryProvider upstream;

    private OrientDbQueryProviderDecorator(QueryProvider upstream) {
        this.upstream = upstream;
    }

    static UnaryOperator<QueryProvider> decorator() {
        return OrientDbQueryProviderDecorator::new;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        return upstream.insertOrUpdate(entity);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return upstream.insertOrUpdate(metaClass, key, entityUpdater);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return upstream.query(query);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return upstream.liveQuery(QueryInfo.<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .skip(query.skip())
                        .limit(query.limit())
                        .build())
                .compose(Notifications.applyQuery(query));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
        return upstream.aggregate(query, aggregator);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update) {
        return upstream.update(update);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return upstream.delete(delete);
    }

}
