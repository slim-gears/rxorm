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

    Single<Supplier<S>> update(S entity);
    Single<Supplier<S>> updateNonRecursive(S entity);

    Maybe<Supplier<S>> update(K key, Function<Maybe<S>, Maybe<S>> updater);
    Maybe<Supplier<S>> updateNonRecursive(K key, Function<Maybe<S>, Maybe<S>> updater);

    Completable update(Iterable<S> entities);
    Completable updateNonRecursive(Iterable<S> entities);

    default Completable update(Observable<S> entities) {
        return entities.flatMapSingle(this::update).ignoreElements();
    }

    default Observable<S> findAll() {
        return findAll((BooleanExpression<S>)null);
    }

    default Observable<S> findAll(BooleanExpression<S> predicate) {
        return query().where(predicate).select().retrieve();
    }

    default Observable<S> findAll(Filter<S> filter) {
        return findAll(filter.toExpression(ObjectExpression.arg(metaClass().asType())).orElse(null));
    }

    default Maybe<S> find(K key) {
        return findFirst(PropertyExpression.ofObject(metaClass().keyProperty()).eq(key));
    }

    default Maybe<S> findFirst(BooleanExpression<S> predicate) {
        return query().where(predicate).limit(1).select().first();
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
