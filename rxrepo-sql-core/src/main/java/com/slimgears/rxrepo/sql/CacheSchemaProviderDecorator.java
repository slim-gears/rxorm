package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import io.reactivex.Completable;
import io.reactivex.Observable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheSchemaProviderDecorator implements SchemaProvider {
    private final SchemaProvider underlyingProvider;
    private final Map<String, Completable> cache = new ConcurrentHashMap<>();

    private CacheSchemaProviderDecorator(SchemaProvider underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
    }

    public static SchemaProvider decorate(SchemaProvider schemaProvider) {
        return new CacheSchemaProviderDecorator(schemaProvider);
    }

    @Override
    public <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass) {
        return cache.computeIfAbsent(
                tableName(metaClass),
                tn -> Completable.defer(() -> createOrUpdateWithReferences(metaClass)).cache());
    }

    @SuppressWarnings("unchecked")
    private <K, T> Completable createOrUpdateWithReferences(MetaClassWithKey<K, T> metaClass) {
        Completable references = Observable.fromIterable(metaClass.properties())
                .filter(p -> p.type().is(HasMetaClassWithKey.class::isAssignableFrom))
                .map(PropertyMeta::type)
                .concatMapCompletable(token -> {
                    MetaClassWithKey<?, ?> meta = (MetaClassWithKey<?, ?>)MetaClasses.forTokenWithKey((TypeToken)token);
                    String tableName = tableName(meta);
                    return !cache.containsKey(tableName)
                            ? createOrUpdate(meta)
                            : Completable.complete();
                });

        return references.andThen(underlyingProvider.createOrUpdate(metaClass));
    }

    @Override
    public <K, T> String tableName(MetaClassWithKey<K, T> metaClass) {
        return underlyingProvider.tableName(metaClass);
    }
}
