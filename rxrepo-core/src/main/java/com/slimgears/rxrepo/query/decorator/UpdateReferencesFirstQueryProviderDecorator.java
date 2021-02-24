package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

@SuppressWarnings({"ReactiveStreamsNullableInLambdaInTransform", "UnstableApiUsage"})
public class UpdateReferencesFirstQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(UpdateReferencesFirstQueryProviderDecorator.class);

    protected UpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return UpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entities).andThen(super.insert(metaClass, entities, true))
                : super.insert(metaClass, entities, false);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entity).andThen(super.insertOrUpdate(metaClass, entity, true))
                : super.insertOrUpdate(metaClass, entity, false);
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, recursive, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> recursive
                        ? insertReferences(metaClass, updatedEntity).andThen(Maybe.just(updatedEntity))
                        : Maybe.just(updatedEntity)));
    }

    private <K, S> Completable insertEntity(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return query(QueryInfo
                .<K, S, S>builder()
                .metaClass(metaClass)
                .limit(1L)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(metaClass.keyOf(entity)))
                .build())
                .firstElement()
                .map(Notification::newValue)
                .<Supplier<S>>map(e -> () -> e)
                .switchIfEmpty(Single.defer(() -> insertOrUpdate(metaClass, entity, recursive)))
                .ignoreElement();
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, S entity) {
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Optional
                                .ofNullable(p.getValue(entity))
                                .map(val -> insertEntity(MetaClasses.forTokenWithKeyUnchecked(p.type()), val, true))
                                .orElseGet(Completable::complete));
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Observable.fromIterable(entities)
                        .filter(e -> Optional.ofNullable(p.getValue(e)).isPresent())
                        .map(p::getValue)
                        .toList()
                        .map(ImmutableSet::copyOf)
                        .flatMapCompletable(refEntities -> refEntities.isEmpty()
                                ? Completable.complete()
                                : insert(MetaClasses.forTokenWithKeyUnchecked(p.type()), refEntities, true)
                                .onErrorResumeNext(e -> e instanceof ConcurrentModificationException
                                        ? Observable.fromIterable(refEntities)
                                        .flatMapCompletable(entity -> insertEntity(MetaClasses.forTokenWithKeyUnchecked(p.type()), entity, true)
                                        .onErrorComplete())
                                        : Completable.error(e))));
    }
//    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
//        Set<Object> isExistingCache = Sets.newConcurrentHashSet();
//        return Observable.fromIterable(metaClass.properties())
//                .filter(PropertyMetas::isReference)
//                .flatMapCompletable(p -> Observable.fromIterable(entities)
//                        .filter(e -> Optional.ofNullable(p.getValue(e)).isPresent())
//                        .map(p::getValue)
//                        .flatMapMaybe(e -> isExisting(isExistingCache, MetaClasses.forTokenWithKeyUnchecked(p.type()), e)
//                                .flatMapMaybe(isExisting -> {
//                                    if (isExisting) {
//                                        return Maybe.empty();
//                                    } else {
//                                        isExistingCache.add(MetaClasses.forTokenWithKeyUnchecked(p.type()).keyOf(e));
//                                        return Maybe.just(e);
//                                    }
//                                }))
//                        .toList()
//                        .flatMapCompletable(refEntities -> refEntities.isEmpty()
//                                ? Completable.complete()
//                                : insert(MetaClasses.forTokenWithKeyUnchecked(p.type()), refEntities, true)));
//    }

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
                .properties(PropertyExpressions.mandatoryProperties(metaClass.asType()).collect(ImmutableSet.toImmutableSet()))
                .build())
                .map(Notification::newValue)
                .firstElement();
    }
}
