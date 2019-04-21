package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.List;

public interface SelectQueryBuilder<K, S extends HasMetaClassWithKey<K, S>>
    extends QueryBuilder<SelectQueryBuilder<K, S>, K, S> {
    SelectQueryBuilder<K, S> skip(long skip);

    <V extends Comparable<V>> SelectQueryBuilder<K, S> orderBy(PropertyExpression<S, S, V> field, boolean ascending);

    SelectQuery<S> select();

    <T> SelectQuery<T> select(ObjectExpression<S, T> expression);

    LiveSelectQuery<S> liveSelect();

    <T> LiveSelectQuery<T> liveSelect(ObjectExpression<S, T> expression);

    default <V extends Comparable<V>> SelectQueryBuilder<K, S> orderBy(PropertyExpression<S, S, V> field) {
        return orderBy(field, true);
    }

    default <V extends Comparable<V>> SelectQueryBuilder<K, S> orderByDescending(PropertyExpression<S, S, V> field) {
        return orderBy(field, false);
    }

    default Single<Long> count() {
        return select().count();
    }

    default Maybe<S> first() {
        return select().first();
    }

    default Observable<Long> observeCount() {
        return liveSelect().count();
    }

    default Observable<S> observeFirst() {
        return liveSelect().first();
    }

    @SuppressWarnings("unchecked")
    default Observable<S> retrieve(PropertyExpression<S, ?, ?>... properties) {
        return select().retrieve(properties);
    }

    @SuppressWarnings("unchecked")
    default Observable<List<Notification<S>>> observe(PropertyExpression<S, ?, ?>... properties) {
        return liveSelect().observe(properties);
    }

    default Observable<S> retrieve() {
        //noinspection unchecked
        return retrieve(new PropertyExpression[0]);
    }

    default Observable<Notification<S>> observe() {
        //noinspection unchecked
        return observe(new PropertyExpression[0]);
    }
}
