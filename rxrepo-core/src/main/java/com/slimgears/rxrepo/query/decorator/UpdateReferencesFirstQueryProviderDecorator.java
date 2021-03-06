package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"UnstableApiUsage"})
public class UpdateReferencesFirstQueryProviderDecorator extends AbstractQueryProviderDecorator {
    protected UpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return UpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entities).andThen(super.insert(metaClass, entities, false))
                : super.insert(metaClass, entities, false);
    }

    @Override
    public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entities).andThen(super.insertOrUpdate(metaClass, entities, false))
                : super.insertOrUpdate(metaClass, entities, false);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entity).andThen(super.insertOrUpdate(metaClass, entity, false))
                : super.insertOrUpdate(metaClass, entity, false);
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return recursive
                ? super.insertOrUpdate(metaClass, key, false, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> insertReferences(metaClass, updatedEntity).andThen(Maybe.just(updatedEntity))))
                : super.insertOrUpdate(metaClass, key, false, entityUpdater);
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertReferences(metaClass, Collections.singleton(entity));
    }

    private <S> Completable insertReferences(MetaClassWithKey<?, S> metaClass, Iterable<S> entities) {
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> insertReferences(p, entities));
    }

    private <K, S, T> Completable insertReferences(PropertyMeta<S, T> property, Iterable<S> entities) {
        MetaClassWithKey<K, T> refMeta = MetaClasses.forTokenWithKeyUnchecked(property.type());
        List<T> referencedObjects = Streams.fromIterable(entities)
                .map(property::getValue)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(this::keyOf, Collectors.reducing((a, b) -> a)))
                .values().stream()
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
        return referencedObjects.isEmpty()
                ? Completable.complete()
                : insertOrUpdate(refMeta, referencedObjects, true);
    }

    @SuppressWarnings("unchecked")
    private <K, S> K keyOf(S entity) {
        return Optional.ofNullable(entity)
                .flatMap(Optionals.ofType(HasMetaClassWithKey.class))
                .map(e -> (HasMetaClassWithKey<K, S>)e)
                .map(e -> e.metaClass().keyOf(entity))
                .orElse(null);
    }
}
