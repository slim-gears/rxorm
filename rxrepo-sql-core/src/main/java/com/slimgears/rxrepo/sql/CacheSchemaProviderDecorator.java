package com.slimgears.rxrepo.sql;

import com.google.common.reflect.TypeToken;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Lazy;
import io.reactivex.Completable;
import io.reactivex.Observable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheSchemaProviderDecorator implements SchemaProvider {
    private final SchemaProvider underlyingProvider;
    private final Lazy<String> dbName;
    private final Map<String, Completable> cache = new ConcurrentHashMap<>();

    private CacheSchemaProviderDecorator(SchemaProvider underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
        this.dbName = Lazy.of(underlyingProvider::databaseName);
    }

    public static SchemaProvider decorate(SchemaProvider schemaProvider) {
        return new CacheSchemaProviderDecorator(schemaProvider);
    }

    @Override
    public String databaseName() {
        return dbName.get();
    }

    @Override
    public <T> Completable createOrUpdate(MetaClass<T> metaClass) {
        return cache.computeIfAbsent(
                tableName(metaClass),
                tn -> Completable.defer(() -> createOrUpdateWithReferences(metaClass)).cache());
    }

    @SuppressWarnings("unchecked")
    private <T> Completable createOrUpdateWithReferences(MetaClass<T> metaClass) {
        Completable references = Observable.fromIterable(metaClass.properties())
                .filter(p -> p.type().isSubtypeOf(HasMetaClass.class))
                .map(PropertyMeta::type)
                .concatMapCompletable(token -> {
                    MetaClass<?> meta = (MetaClass<?>)MetaClasses.forToken((TypeToken)token);
                    String tableName = tableName(meta);
                    return !cache.containsKey(tableName)
                            ? createOrUpdate(meta)
                            : Completable.complete();
                });

        return references.andThen(underlyingProvider.createOrUpdate(metaClass));
    }

    @Override
    public <T> String tableName(MetaClass<T> metaClass) {
        return underlyingProvider.tableName(metaClass);
    }
}
