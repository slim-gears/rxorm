package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

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
        return upstream.liveQuery(query.toBuilder()
                .properties(ImmutableList.of())
                .build())
                .filter(fieldsFilter(query.properties()));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Single<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R, ?> aggregator) {
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

    private <T> Predicate<Notification<T>> fieldsFilter(Collection<PropertyExpression<T, ?, ?>> properties) {
        List<java.util.function.Function<T, ?>> propertyMetas = properties.stream()
                .map(Expressions::compile)
                .collect(Collectors.toList());

        return n -> fieldsChanged(n, propertyMetas);
    }

    private <T> boolean fieldsChanged(Notification<T> notification, List<java.util.function.Function<T, ?>> properties) {
        if (!notification.isModify()) {
            return true;
        }

        return properties
                .stream()
                .anyMatch(p -> !Objects.equals(
                        p.apply(notification.oldValue()),
                        p.apply(notification.newValue())));
    }
}
