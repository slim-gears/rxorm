package com.slimgears.rxrepo.query.decorator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class UpdateReferencesFirstQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(UpdateReferencesFirstQueryProviderDecorator.class);

    private UpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return UpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return insertReferences(metaClass, entities)
                .andThen(super.insert(metaClass, entities));
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertReferences(metaClass, entity).andThen(super.insertOrUpdate(metaClass, entity));
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> insertReferences(metaClass, updatedEntity)
                        .andThen(Maybe.just(updatedEntity))));
    }

    private <K, S> Completable insertEntity(MetaClassWithKey<K, S> metaClass, S entity) {
        return query(QueryInfo
            .<K, S, S>builder()
            .metaClass(metaClass)
            .limit(1L)
            .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(metaClass.keyOf(entity)))
            .build())
            .firstElement()
            .switchIfEmpty(Single.defer(() -> insertOrUpdate(metaClass, entity)))
            .ignoreElement();
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, S entity) {
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Optional
                                .ofNullable(p.getValue(entity))
                                .map(val -> insertEntity(MetaClasses.forTokenWithKeyUnchecked(p.type()), val))
                                .orElseGet(Completable::complete));
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        Set<Object> isExistingCache = Sets.newConcurrentHashSet();
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Observable.fromIterable(entities)
                        .filter(e -> Optional.ofNullable(p.getValue(e)).isPresent())
                        .map(p::getValue)
                        .flatMapMaybe(e -> isExisting(isExistingCache, MetaClasses.forTokenWithKeyUnchecked(p.type()), e)
                                .flatMapMaybe(isExisting -> {
                                    if (isExisting) {
                                        return Maybe.empty();
                                    } else {
                                        isExistingCache.add(MetaClasses.forTokenWithKeyUnchecked(p.type()).keyOf(e));
                                        return Maybe.just(e);
                                    }
                                }))
                        .toList()
                        .flatMapCompletable(refEntities -> refEntities.isEmpty()
                                ? Completable.complete()
                                : super.insert(MetaClasses.forTokenWithKeyUnchecked(p.type()), refEntities)));
    }

    private <K, S> Single<Boolean> isExisting(Set<Object> isExistingCache, MetaClassWithKey<K, S> metaClass, S entity) {
        K key = metaClass.keyOf(entity);
        return isExistingCache.contains(key) ?
                Single.just(true) :
                existingEntity(metaClass, entity).map(e -> true)
                        .toSingle(false)
                        .doOnSuccess(isExisting -> isExistingCache.add(key));
    }

    private <K, S> Maybe<S> existingEntity(MetaClassWithKey<K, S> metaClass, S entity) {
        return query(QueryInfo.<K, S, S>builder()
                .metaClass(metaClass)
                .limit(1L)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(metaClass.keyOf(entity)))
                .build())
                .firstElement();
    }
}
