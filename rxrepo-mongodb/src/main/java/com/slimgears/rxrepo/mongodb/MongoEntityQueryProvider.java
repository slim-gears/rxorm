package com.slimgears.rxrepo.mongodb;

import com.google.common.reflect.TypeToken;
import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.slimgears.rxrepo.encoding.MetaClassFieldMapper;
import com.slimgears.rxrepo.encoding.MetaDocument;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.EntityQueryProvider;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class MongoEntityQueryProvider<K, S> implements EntityQueryProvider<K, S> {
    private final static Logger log = LoggerFactory.getLogger(MongoEntityQueryProvider.class);
    private final MetaClassWithKey<K, S> metaClass;
    private final Lazy<MongoCollection<Document>> objectCollection;
    private final Lazy<MongoCollection<Document>> notificationCollection;
    private final Lazy<Codec<S>> codec;
    private final Lazy<Codec<Document>> docCodec;
    private final CodecRegistry codecRegistry;
    private final MetaClassFieldMapper fieldMapper;

    MongoEntityQueryProvider(MetaClassWithKey<K, S> metaClass, MongoDatabase database, MetaClassFieldMapper fieldMapper) {
        this.metaClass = metaClass;
        this.codecRegistry = database.getCodecRegistry();
        this.codec = Lazy.of(() -> codecRegistry.get(metaClass.asClass()));
        this.docCodec = Lazy.of(() -> codecRegistry.get(Document.class));
        this.fieldMapper = fieldMapper;
        this.objectCollection = Lazy.of(() -> database.getCollection(metaClass.simpleName()));
        this.notificationCollection = Lazy.of(() -> database.getCollection(metaClass.simpleName() + ".updates"));
    }

    private Maybe<Document> findDocument(K key) {
        return Observable.fromPublisher(objectCollection.get()
                .aggregate(MongoPipeline.builder()
                        .lookupAndUnwindReferences(metaClass)
                        .match(MongoPipeline.filterForKey(key))
                        .limit(1L)
                        .build()))
                .firstElement();
    }

    @Override
    public Completable insert(Iterable<S> entities) {
        List<Document> documents = Streams
                .fromIterable(entities)
                .map(e -> objectToDocument(e, 0))
                .collect(Collectors.toList());

        return Completable
                .fromPublisher(objectCollection.get().insertMany(documents))
                .doOnComplete(() -> log.debug("Insert of {} documents complete", documents.size()))
                .onErrorResumeNext(e -> Completable.error(convertError(e)));
    }

    @Override
    public MetaClassWithKey<K, S> metaClass() {
        return metaClass;
    }

    @Override
    public Maybe<S> insertOrUpdate(K key, Function<Maybe<S>, Maybe<S>> update) {
        AtomicLong version = new AtomicLong();
        AtomicReference<S> oldObject = new AtomicReference<>();
        AtomicReference<S> newObject = new AtomicReference<>();
        AtomicReference<Document> oldDoc = new AtomicReference<>();
        AtomicReference<Document> newDoc = new AtomicReference<>();
        return findDocument(key)
                .doOnSuccess(oldDoc::set)
                .doOnSuccess(doc -> version.set(doc.getLong(fieldMapper.versionField())))
                .map(this::objectFromDocument)
                .doOnSuccess(oldObject::set)
                .map(Maybe::just)
                .flatMap(update)
                .doOnSuccess(newObject::set)
                .flatMap(newObj -> Objects.equals(newObj, oldObject.get())
                        ? Maybe.just(oldObject.get())
                        : Maybe.just(newObj)
                        .map(obj -> objectToDocument(obj, version.get() + 1))
                        .doOnSuccess(newDoc::set)
                        .doOnSuccess(doc -> log.trace("Updating object: {}", doc))
                        .flatMap(doc -> Single
                            .fromPublisher(objectCollection.get()
                                    .replaceOne(
                                            MongoPipeline.filterForKeyAndVersion(key, version.get()),
                                            doc))
                            .doOnSuccess(res -> log.trace("Update result: {}", res))
                            .map(UpdateResult::getMatchedCount)
                            .flatMapMaybe(c -> c == 1
                                    ? Maybe.just(newObject.get())
                                    : Maybe.error(
                                            new ConcurrentModificationException("Concurrent modification detected: version " +
                                                    version.get() +
                                                    " of object (id: " + key + ") not found")))
                            .flatMap(obj -> publish(oldDoc.get(), newDoc.get())
                                    .andThen(Maybe.just(obj)))))
                .switchIfEmpty(Maybe
                        .defer(() -> update.apply(Maybe.empty()))
                        .doOnSuccess(newObject::set)
                        .doOnSuccess(doc -> log.trace("Creating new object: {}", doc))
                        .map(obj -> objectToDocument(obj, version.get()))
                        .flatMap(doc -> Single.fromPublisher(objectCollection.get()
                                .insertOne(doc))
                                .doOnSuccess(res -> log.trace("Insert result: {}", res))
                                .toMaybe()
                                .map(res -> newObject.get())
                                .onErrorResumeNext((Throwable e) -> Maybe.error(convertError(e)))))
                .doOnSuccess(obj -> log.trace("Final object after update/insert: {}", obj))
                .doOnError(e -> log.trace("Could not update object: ", e));
    }

    @Override
    public <T> Observable<T> query(QueryInfo<K, S, T> query) {
        return queryDocuments(query)
                .doOnNext(doc -> log.debug("Retrieved document: {}", doc))
                .map(doc -> objectFromDocument(doc, query.objectType()));
    }

    @Override
    public <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        AggregatePublisher<MetaDocument> publisher = objectCollection.get()
                .aggregate(MongoPipeline.aggregationPipeline(query, aggregator), MetaDocument.class);

        TypeToken<R> resultType = aggregator.objectType(query.objectType());
        return Observable.fromPublisher(publisher)
                .doOnNext(doc -> log.debug("Retrieved document: {}", doc))
                .map(doc -> doc.get(MongoPipeline.aggregationField, resultType))
                .firstElement();
    }

    private Observable<Document> queryDocuments(QueryInfo<K, S, ?> query) {
        return Observable
                .fromPublisher(objectCollection.get()
                .aggregate(MongoPipeline.aggregationPipeline(query)));
    }

    @Override
    public <T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        java.util.function.Function<S, T> mapper = Expressions.compile(query.mapping());

        Observable<Notification<S>> modifications = Observable.fromPublisher(notificationCollection.get().watch())
                .map(ChangeStreamDocument::getFullDocument)
                .doOnNext(d -> log.trace("New update: {}", d.toJson()))
                .map(this::notificationFromDocument)
                .filter(n -> !Objects.equals(n.oldValue(), n.newValue()));

        Observable<Notification<S>> insertions = Observable
                .fromPublisher(objectCollection
                        .get()
                        .watch())
                .doOnNext(d -> log.trace("Change detected: {}", d))
                .flatMapMaybe(this::notificationFromChangeDocument);

        return modifications.mergeWith(insertions)
                .map(n -> n.map(mapper));
    }

    private Completable publish(Document oldDoc, Document newDoc) {
        return Completable.fromPublisher(notificationCollection.get()
                .insertOne(createNotification(oldDoc, newDoc)));
    }

    private Document createNotification(Document oldDoc, Document newDoc) {
        Object id = Optionals.or(
                () -> Optional.ofNullable(newDoc),
                () -> Optional.ofNullable(oldDoc))
                .map(doc -> doc.get("_id"))
                .orElse(null);

        return new Document("key", id)
                .append("oldValue", oldDoc)
                .append("newValue", newDoc);
    }

    private Completable publishNotification(Document notificationDocument) {
        return Completable
                .fromPublisher(notificationCollection.get()
                .insertOne(notificationDocument));
    }

    @Override
    public Single<Integer> update(UpdateInfo<K, S> updateInfo) {
        return Single.error(() -> new UnsupportedOperationException("Not supported yet"));
//        return Observable.fromPublisher(objectCollection.get()
//                .updateMany(
//                        MongoPipeline.expr(updateInfo.predicate()),
//                        MongoPipeline.setFields(updateInfo.propertyUpdates())))
//                .map(UpdateResult::getModifiedCount)
//                .firstElement()
//                .map(Long::intValue)
//                .toSingle(0);
    }

    @Override
    public Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return queryDocuments(QueryInfo
                .<K, S, S>builder()
                .metaClass(deleteInfo.metaClass())
                .predicate(deleteInfo.predicate())
                .limit(deleteInfo.limit())
                .build())
                .map(doc -> createNotification(doc, doc))
                .flatMapCompletable(this::publishNotification)
                .andThen(Observable.fromPublisher(objectCollection.get()
                        .deleteMany(MongoPipeline.expr(deleteInfo.predicate())))
                        .map(DeleteResult::getDeletedCount)
                        .firstElement()
                        .map(Long::intValue)
                        .toSingle(0));
    }

    @Override
    public Completable drop() {
        return Completable.fromPublisher(objectCollection.get().drop());
    }

    private static Throwable convertError(Throwable e) {
        return isDuplicateKeyException(e)
                ? new ConcurrentModificationException(e)
                : e;
    }

    private static boolean isDuplicateKeyException(Throwable e) {
        return e instanceof DuplicateKeyException ||
                (e instanceof MongoWriteException && ((MongoWriteException)e).getError().getCategory() == ErrorCategory.DUPLICATE_KEY) ||
                (e instanceof MongoBulkWriteException && ((MongoBulkWriteException)e)
                        .getWriteErrors()
                        .stream()
                        .map(BulkWriteError::getCategory)
                        .anyMatch(ErrorCategory.DUPLICATE_KEY::equals));
    }

    private Notification<S> notificationFromDocument(Document document) {
        return Notification.ofModified(
                toObject(document.get("oldValue"), metaClass.asType()),
                toObject(document.get("newValue"), metaClass.asType()));
    }

    private <T> T toObject(Object object, TypeToken<T> type) {
        if (object instanceof Document) {
            return objectFromDocument((Document)object, type);
        }

        if (type.getRawType().isInstance(object)) {
            return TypeTokens.asClass(type).cast(object);
        }

        Codec<T> codec = codecRegistry.get(TypeTokens.asClass(type));
        BsonDocument bson = toBson(new Document().append("value", object));
        BsonReader reader = bson.asBsonReader();
        reader.readStartDocument();
        reader.readName();
        T res = codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();
        return res;
    }

    private Maybe<Notification<S>> notificationFromChangeDocument(ChangeStreamDocument<Document> changeDoc) {
        if (changeDoc.getOperationType() == OperationType.INSERT) {
            S object = Optional
                    .ofNullable(changeDoc.getFullDocument())
                    .map(this::objectFromDocument)
                    .orElse(null);
            return Maybe.just(Notification.ofCreated(object));
        } else if (changeDoc.getOperationType() == OperationType.DELETE) {
            Object key = Optional.of(changeDoc.getDocumentKey())
                    .map(doc -> doc.get("_id"))
                    .orElse(null);

            return Observable.fromPublisher(notificationCollection.get()
                    .aggregate(MongoPipeline.builder()
                            .match(MongoPipeline.filterForField("key", key))
                            .replaceRoot("$newValue")
                            .sort(new Document(fieldMapper.versionField(), -1))
                            .limit(1L)
                            .build()))
                    .firstElement()
                    .map(this::objectFromDocument)
                    .map(Notification::ofDeleted);
        }
        return Maybe.empty();
    }

    private Document objectToDocument(S obj, long version) {
        BsonDocument bson = new BsonDocument();
        codec.get().encode(new BsonDocumentWriter(bson), obj, EncoderContext.builder().build());
        bson.append(fieldMapper.versionField(), new BsonInt64(version));
        return fromBson(bson);
    }

    private S objectFromDocument(Document doc) {
        return codec.get().decode(toBson(doc).asBsonReader(), DecoderContext.builder().build());
    }

    private BsonDocument toBson(Document doc) {
        return doc.toBsonDocument(BsonDocument.class, codecRegistry);
    }

    private Document fromBson(BsonDocument bson) {
        return docCodec.get().decode(bson.asBsonReader(), DecoderContext.builder().build());
    }

    @SuppressWarnings("unchecked")
    private <T> T objectFromDocument(Document doc, TypeToken<T> objectType) {
        if (!PropertyMetas.hasMetaClass(objectType)) {
            return (T)doc.get(MongoPipeline.valueField);
        }
        BsonDocument bsonDoc = doc.toBsonDocument(BsonDocument.class, codecRegistry);
        BsonReader reader = bsonDoc.asBsonReader();
        Codec<T> codec = codecRegistry.get(TypeTokens.asClass(objectType));
        return codec.decode(reader, DecoderContext.builder().build());
    }

    private <T> T fromAggregation(BsonDocument doc, TypeToken<T> type) {
        Decoder<T> decoder = codecRegistry.get(TypeTokens.asClass(type));
        return new AggregationResultDecoder<>(decoder).decode(doc.asBsonReader(), AggregationResultDecoder.defaultContext);
    }

    static class AggregationResultDecoder<T> implements Decoder<T>  {
        private final static DecoderContext defaultContext = DecoderContext.builder().build();
        private final Decoder<T> typeDecoder;

        AggregationResultDecoder(Decoder<T> typeDecoder) {
            this.typeDecoder = typeDecoder;
        }

        @Override
        public T decode(BsonReader reader, DecoderContext decoderContext) {
            reader.readStartDocument();
            String name;
            T value = null;
            do {
                name = reader.readName();
                if (name.equals(MongoPipeline.aggregationField)) {
                    value = typeDecoder.decode(reader, decoderContext);
                } else {
                    reader.skipValue();
                }
            } while (reader.getCurrentBsonType() != BsonType.END_OF_DOCUMENT);
            reader.readEndDocument();
            return value;
        }

    }
}
