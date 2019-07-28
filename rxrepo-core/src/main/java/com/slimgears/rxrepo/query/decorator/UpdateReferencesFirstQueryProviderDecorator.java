package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
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

public class UpdateReferencesFirstQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(UpdateReferencesFirstQueryProviderDecorator.class);

    private UpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator decorator() {
        return UpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<S> insertOrUpdate(S entity) {
        K key = HasMetaClassWithKey.keyOf(entity);
        return insertOrUpdate(entity.metaClass(), key, val -> val
                .map(e -> e.merge(entity))
                .switchIfEmpty(Maybe.just(entity)))
                .toSingle();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> insertOrUpdateReferences(updatedEntity)
                        .andThen(Maybe.just(updatedEntity))));
    }

    @SuppressWarnings("unchecked")
    private <S> Completable insertOrUpdateEntity(S entity) {
        return insertOrUpdate((HasMetaClassWithKey)entity)
                .doOnSubscribe(d -> log.debug("Inserting entity: {}", entity))
                .ignoreElement();
    }

    private <S> Completable insertOrUpdateReferences(S entity) {
        MetaClass<S> metaClass = MetaClasses.forClassUnchecked(entity.getClass());
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Optional
                                .ofNullable(p.getValue(entity))
                                .map(val -> insertOrUpdateReferences(val).andThen(insertOrUpdateEntity(val)))
                                .orElseGet(Completable::complete));
    }
}
