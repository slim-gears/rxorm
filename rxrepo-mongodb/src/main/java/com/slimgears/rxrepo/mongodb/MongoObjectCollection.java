package com.slimgears.rxrepo.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.internal.ObjectConstantExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Lazy;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class MongoObjectCollection<K, S extends HasMetaClassWithKey<K, S>> {
    private final static Logger log = LoggerFactory.getLogger(MongoObjectCollection.class);
    private final MetaClass<S> metaClass;
    private final Lazy<MongoCollection<BsonDocument>> objectCollection;
    private final Lazy<MongoCollection<Document>> notificationCollection;
    private final Codec<Document> documentCodec;
    private final Codec<S> codec;
    private final CodecRegistry codecRegistry;

    MongoObjectCollection(MetaClass<S> metaClass, MongoDatabase database) {
        this.metaClass = metaClass;
        this.codecRegistry = database.getCodecRegistry();
        this.codec = codecRegistry.get(metaClass.asClass());
        this.documentCodec = codecRegistry.get(Document.class);
        this.objectCollection = Lazy.of(() -> database.getCollection(metaClass.simpleName(), BsonDocument.class));
        this.notificationCollection = Lazy.of(() -> database.getCollection(metaClass.simpleName() + ".updates"));
    }

    Maybe<S> insertOrUpdate(K key, Function<Maybe<S>, Maybe<S>> update) {
        AtomicLong version = new AtomicLong();
        AtomicReference<S> oldObject = new AtomicReference<>();
        AtomicReference<S> newObject = new AtomicReference<>();
        return Observable
                .fromPublisher(objectCollection.get()
                .find(MongoQueries.filterForKey(key)))
                .firstElement()
                .doOnSuccess(doc -> version.set(doc.getInt64("_version").longValue()))
                .map(this::objectFromDocument)
                .doOnSuccess(oldObject::set)
                .map(Maybe::just)
                .flatMap(update)
                .doOnSuccess(newObject::set)
                .map(obj -> objectToDocument(obj, version.get() + 1))
                .doOnSuccess(doc -> log.debug("Updating object: {}", doc))
                .flatMap(doc -> Single
                        .fromPublisher(objectCollection.get()
                                .replaceOne(
                                        MongoQueries.filterForKeyAndVersion(key, version.get()),
                                        doc))
                        .doOnSuccess(res -> log.debug("Update result: {}", res))
                        .map(UpdateResult::getMatchedCount)
                        .flatMapMaybe(c -> c == 1
                                ? Maybe.just(newObject.get())
                                : Maybe.error(
                                        new ConcurrentModificationException("Concurrent modification detected: version " +
                                                version.get() +
                                                " of object id " + key + " not found")))
                        .flatMap(obj -> publish(Notification
                                .ofModified(oldObject.get(), obj))
                                .andThen(Maybe.just(obj))))
                .switchIfEmpty(Maybe
                        .defer(() -> update.apply(Maybe.empty()))
                        .doOnSuccess(newObject::set)
                        .doOnSuccess(doc -> log.debug("Creating new object: {}", doc))
                        .map(obj -> objectToDocument(obj, version.get()))
                        .flatMap(doc -> Single.fromPublisher(objectCollection.get()
                                .insertOne(doc))
                                .doOnSuccess(res -> log.debug("Insert result: {}", res))
                                .toMaybe()
                                .map(res -> newObject.get())
                                .onErrorResumeNext((Throwable e) -> Maybe.error(convertError(e)))))
                .doOnSuccess(obj -> log.debug("Final object after update/insert: {}", obj))
                .doOnError(e -> log.debug("Could not update object: ", e));
    }

    <T> Observable<T> query(QueryInfo<K, S, T> query) {
//        FindPublisher<BsonDocument> findPublisher = objectCollection.get()
//                .find()
//                .filter(MongoQueries.expr(query.predicate()))
//                .sort(MongoQueries.toSorting(query.sorting()))
//                .projection(MongoQueries.toProjection(query.properties()));
//
//
//        findPublisher = Optional.ofNullable(query.limit())
//                .map(Long::intValue)
//                .map(findPublisher::limit)
//                .orElse(findPublisher);
//
//        findPublisher = Optional.ofNullable(query.skip())
//                .map(Long::intValue)
//                .map(findPublisher::skip)
//                .orElse(findPublisher);
        AggregatePublisher<Document> publisher = objectCollection.get()
                .aggregate(MongoQueries.aggregationPipeline(query));

        return Observable
                .fromPublisher(publisher)
                .doOnNext(doc -> log.debug("Retrieved document: {}", doc))
                .map(objectFromDocument(query.objectType()));
    }

    <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        AggregatePublisher<Document> publisher = objectCollection.get()
                .aggregate(MongoQueries.aggregationPipeline(query));

        Expressions.compileRx(aggregator
                .apply(ObjectExpression.objectArg(TypeToken.ofParameterized(List.class, query.objectType()))));

        return Observable.fromPublisher(publisher)
                .doOnNext(doc -> log.debug("Retrieved document: {}", doc))
                .map(objectFromDocument(query.objectType()))
                .toList()
                .toMaybe()
                .map(l -> (Collection<T>)l)
                .map(l -> ObjectConstantExpression.<T, Collection<T>>create(Expression.Type.Constant, l))
                .map(aggregator::apply)
                .map(Expressions::compile)
                .map(exp -> exp.apply(null));
    }

    Observable<Notification<S>> liveQuery() {
        Observable<Notification<S>> modifications = Observable.fromPublisher(notificationCollection.get().watch())
                .map(ChangeStreamDocument::getFullDocument)
                .map(this::notificationFromDocument);

        Observable<Notification<S>> insertionsAndDeletions = Observable
                .fromPublisher(objectCollection
                        .get()
                        .watch(BsonDocument.class))
                .flatMapMaybe(this::notificationFromChangeDocument);

        return modifications.mergeWith(insertionsAndDeletions);
    }

    private Completable publish(Notification<S> notification) {
        return Completable.fromPublisher(notificationCollection.get()
                .insertOne(notificationToDocument(notification)));
    }

    Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return Observable.fromPublisher(objectCollection.get()
                .deleteMany(MongoQueries.expr(deleteInfo.predicate())))
                .map(DeleteResult::getDeletedCount)
                .firstElement()
                .map(Long::intValue)
                .toSingle(0);
    }

    private static Throwable convertError(Throwable e) {
        return (isDuplicateKeyException(e))
                ? new ConcurrentModificationException(e)
                : e;
    }

    private static boolean isDuplicateKeyException(Throwable e) {
        return e instanceof DuplicateKeyException ||
                (e instanceof MongoWriteException && ((MongoWriteException)e).getError().getCategory() == ErrorCategory.DUPLICATE_KEY);
    }

    private Document notificationToDocument(Notification<S> notification) {
        return new Document()
                .append("oldValue", notification.oldValue())
                .append("newValue", notification.newValue());
    }

    private Notification<S> notificationFromDocument(Document document) {
        return Notification.ofModified(
                document.get("oldValue", metaClass.asClass()),
                document.get("newValue", metaClass.asClass()));
    }

    private Maybe<Notification<S>> notificationFromChangeDocument(ChangeStreamDocument<BsonDocument> changeDoc) {
        S object = Optional
                .ofNullable(changeDoc.getFullDocument())
                .map(this::objectFromDocument)
                .orElse(null);

        if (changeDoc.getOperationType() == OperationType.INSERT) {
            return Maybe.just(Notification.ofCreated(object));
        } else if (changeDoc.getOperationType() == OperationType.DELETE) {
            return object != null
                    ? Maybe.just(Notification.ofDeleted(object))
                    : Maybe.empty();
        }
        return Maybe.empty();
    }

    private BsonDocument objectToDocument(S obj, long version) {
        BsonDocument doc = new BsonDocument();
        codec.encode(new BsonDocumentWriter(doc), obj, EncoderContext.builder().build());
        doc.append("_version", new BsonInt64(version));
        return doc;
    }

    private S objectFromDocument(BsonDocument bsonDoc) {
        return codec.decode(bsonDoc.asBsonReader(), DecoderContext.builder().build());
    }

    private S objectFromDocument(Document doc) {
        return objectFromDocument(doc.toBsonDocument(BsonDocument.class, codecRegistry));
    }

    private <T> Function<Document, T> objectFromDocument(TypeToken<T> objectType) {
        return doc -> {
            BsonReader reader = doc.toBsonDocument(BsonDocument.class, codecRegistry).asBsonReader();
            Codec<T> codec = codecRegistry.get(objectType.asClass());
            return codec.decode(reader, DecoderContext.builder().build());
        };
    }
}
