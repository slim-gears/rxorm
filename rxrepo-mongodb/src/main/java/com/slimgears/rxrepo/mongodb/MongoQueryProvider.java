package com.slimgears.rxrepo.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.mongodb.codecs.Codecs;
import com.slimgears.rxrepo.mongodb.codecs.MetaClassCodecProvider;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.*;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.bson.codecs.configuration.CodecRegistries;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MongoQueryProvider implements QueryProvider {
    private final MongoClient client;
    private final MongoDatabase database;
    private final Map<String, MongoObjectCollection<?, ?>> collectionCache = new ConcurrentHashMap<>();
    private final Scheduler scheduler = Schedulers.io();
    private final ReferencedObjectResolver objectResolver = new ReferencedObjectResolver() {
        @Override
        public <K, S extends HasMetaClassWithKey<K, S>> S resolve(MetaClassWithKey<K, S> metaClass, K key) {
            return collection(metaClass)
                    .query(QueryInfo.<K, S, S>builder()
                            .metaClass(metaClass)
                            .predicate(PropertyExpression.ofObject(ObjectExpression.arg(metaClass.objectClass()), metaClass.keyProperty()).eq(key))
                            .limit(1L)
                            .build())
                    .firstElement()
                    .map(Optional::of)
                    .blockingGet(Optional.empty())
                    .orElse(null);
        }
    };

    MongoQueryProvider(String connectionString, String dbName) {
        this.client = MongoClients.create(MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .build());
        this.database = client.getDatabase(dbName)
                .withCodecRegistry(CodecRegistries.fromProviders(
                        Codecs.discoverProviders(),
                        MetaClassCodecProvider.create(objectResolver)));
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return collection(metaClass)
                .insertOrUpdate(key, m -> entityUpdater.apply(m).subscribeOn(scheduler));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return collection(query.metaClass())
                .query(query)
                .subscribeOn(scheduler);
    }


    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        java.util.function.Function<S, T> mapper = Expressions.compile(query.mapping());
        return collection(query.metaClass())
                .liveQuery()
                .map(n -> n.map(mapper));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return collection(query.metaClass())
                .aggregate(query, aggregator)
                .subscribeOn(scheduler);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Observable<S> update(UpdateInfo<K, S> update) {
        return Observable.empty();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return collection(delete.metaClass())
                .delete(delete)
                .subscribeOn(scheduler);
    }

    @Override
    public Completable drop() {
        return Completable.fromPublisher(database.drop())
                .subscribeOn(scheduler);
    }

    @SuppressWarnings("unchecked")
    private <K, S extends HasMetaClassWithKey<K, S>> MongoObjectCollection<K, S> collection(MetaClassWithKey<K, S> metaClass) {
        return (MongoObjectCollection<K, S>) collectionCache
                .computeIfAbsent(metaClass.simpleName(), n -> retrieveCollection(metaClass));
    }

    private <K, S extends HasMetaClassWithKey<K, S>> MongoObjectCollection<K, S> retrieveCollection(MetaClassWithKey<K, S> metaClass) {
        return new MongoObjectCollection<>(metaClass, database);
    }
}
