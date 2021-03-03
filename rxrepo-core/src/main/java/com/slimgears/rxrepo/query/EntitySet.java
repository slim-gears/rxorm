package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Arrays;
import java.util.function.Supplier;

@SuppressWarnings({"unused", "unchecked"})
public interface EntitySet<K, S> {
    MetaClassWithKey<K, S> metaClass();
    EntityDeleteQuery<S> delete();
    SelectQueryBuilder<S> query();

    EntityUpdateQuery<S> update();

    Single<Single<S>> update(S entity);
    Single<Single<S>> updateNonRecursive(S entity);

    Maybe<Single<S>> update(K key, Function<Maybe<S>, Maybe<S>> updater);
    Maybe<Single<S>> updateNonRecursive(K key, Function<Maybe<S>, Maybe<S>> updater);

    Completable update(Iterable<S> entities);
    Completable updateNonRecursive(Iterable<S> entities);

    default Completable update(Observable<S> entities) {
        return entities.flatMapSingle(this::update).ignoreElements();
    }

    default Observable<S> findAll(PropertyExpression<S, ?, ?>... properties) {
        return findAll((BooleanExpression<S>)null, properties);
    }

    default Observable<S> findAll(BooleanExpression<S> predicate, PropertyExpression<S, ?, ?>... properties) {
        return query().where(predicate).select().properties(properties).retrieve();
    }

    default Observable<S> findAll(Filter<S> filter, PropertyExpression<S, ?, ?>... properties) {
        return findAll(filter.toExpression(ObjectExpression.arg(metaClass().asType())).orElse(null), properties);
    }

    default Maybe<S> find(K key, PropertyExpression<S, ?, ?>... properties) {
        return findFirst(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key), properties);
    }

    default Maybe<S> findFirst(BooleanExpression<S> predicate, PropertyExpression<S, ?, ?>... properties) {
        return query().where(predicate).limit(1).select().properties(properties).first();
    }

    default Completable update(S[] entities) {
        return update(Arrays.asList(entities));
    }
    default Completable updateNonRecursive(S[] entities) {
        return updateNonRecursive(Arrays.asList(entities));
    }

    default Completable clear() {
        return deleteAll(null);
    }

    default Completable delete(K key) {
        return delete()
                .where(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key))
                .execute()
                .ignoreElement();
    }

    default Completable delete(K[] keys) {
        return deleteAll(PropertyExpression.ofObject(metaClass().keyProperty()).in(keys));
    }

    default Completable deleteAll(BooleanExpression<S> predicate) {
        return delete()
                .where(predicate)
                .execute()
                .ignoreElement();
    }

    default Observable<Notification<S>> observe() {
        return query().liveSelect().observe();
    }

    default Observable<Notification<S>> observe(PropertyExpression<S, ?, ?>... properties) {
        return query().liveSelect().observe(properties);
    }

    default Observable<Notification<S>> queryAndObserve() {
        return query().liveSelect().queryAndObserve();
    }

    default Observable<Notification<S>> queryAndObserve(PropertyExpression<S, ?, ?>... properties) {
        return query().liveSelect().queryAndObserve(properties);
    }

    default <R> Observable<R> observeAs(QueryTransformer<S, R> queryTransformer) {
        return query().liveSelect().observeAs(queryTransformer);
    }

    default <R> Observable<R> observeAs(QueryTransformer<S, R> queryTransformer, PropertyExpression<S, ?, ?>... properties) {
        return query().liveSelect().observeAs(queryTransformer, properties);
    }
}
