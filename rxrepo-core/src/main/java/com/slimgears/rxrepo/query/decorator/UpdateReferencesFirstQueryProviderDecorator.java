package com.slimgears.rxrepo.query.decorator;

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
        return Observable.fromIterable(entities)
                .concatMapEager(e -> insertOrUpdateReferences(metaClass, e).andThen(Observable.just(e)))
                .ignoreElements()
                .andThen(super.insert(metaClass, entities));
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertOrUpdateReferences(metaClass, entity).andThen(super.insertOrUpdate(metaClass, entity));
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> insertOrUpdateReferences(metaClass, updatedEntity)
                        .andThen(Maybe.just(updatedEntity))));
    }

    private <K, S> Completable insertOrUpdateEntity(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertOrUpdate(metaClass, entity)
                .doOnSubscribe(d -> log.debug("Inserting entity: {}", entity))
                .ignoreElement();
    }

    private <K, S> Completable insertOrUpdateReferences(MetaClassWithKey<K, S> metaClass, S entity) {
        return Observable.fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .flatMapCompletable(p -> Optional
                                .ofNullable(p.getValue(entity))
                                .map(val -> insertOrUpdateReferences(MetaClasses.forTokenWithKeyUnchecked(p.type()), val)
                                        .andThen(insertOrUpdateEntity(MetaClasses.forTokenWithKeyUnchecked(p.type()), val)))
                                .orElseGet(Completable::complete));
    }
}
