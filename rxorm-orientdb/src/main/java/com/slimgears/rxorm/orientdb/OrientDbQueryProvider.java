package com.slimgears.rxorm.orientdb;

import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.Aggregator;
import com.slimgears.util.repository.expressions.ObjectExpression;
import com.slimgears.util.repository.query.HasEntityMeta;
import com.slimgears.util.repository.query.HasMapping;
import com.slimgears.util.repository.query.HasPagination;
import com.slimgears.util.repository.query.HasPredicate;
import com.slimgears.util.repository.query.HasProperties;
import com.slimgears.util.repository.query.HasPropertyUpdates;
import com.slimgears.util.repository.query.HasSortingInfo;
import com.slimgears.util.repository.query.Notification;
import com.slimgears.util.repository.query.QueryInfo;
import com.slimgears.util.repository.query.QueryProvider;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleTransformer;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OrientDbQueryProvider implements QueryProvider {
    private final static Logger log = Logger.getLogger(OrientDbQueryProvider.class.getName());
    private final Provider<ODatabaseSession> dbSessionProvider;
    private final ObjectConverter converter;
    private final Map<String, OClass> classMap = new HashMap<>();
    private final LoadingCache<KeyInfo, Maybe<ORecordId>> keyCache = CacheBuilder.newBuilder()
            .maximumSize(2000)
            .build(new CacheLoader<KeyInfo, Maybe<ORecordId>>() {
                @Override
                public Maybe<ORecordId> load(@Nonnull KeyInfo key) {
                    return loadId(key);
                }
            });

    @AutoValue
    static abstract class KeyInfo {
        public abstract String className();
        public abstract String key();

        public static <K, T, B extends BuilderPrototype<T, B>> KeyInfo create(MetaClassWithKey<K, T, B> meta, K key) {
            return new AutoValue_OrientDbQueryProvider_KeyInfo(
                    toClassName(meta.objectClass()),
                    toKey(key));
        }

        public static <K, T, B extends BuilderPrototype<T, B>> KeyInfo fromObject(HasMetaClassWithKey<K, T, B> obj) {
            //noinspection unchecked
            return new AutoValue_OrientDbQueryProvider_KeyInfo(
                    toClassName(obj.metaClass().objectClass()),
                    toKey(obj.metaClass().keyProperty().getValue((T)obj)));
        }
    }

    @Inject
    public OrientDbQueryProvider(Provider<ODatabaseSession> dbSessionProvider) {
        this.dbSessionProvider = ThreadLocalProvider.of(dbSessionProvider);
        this.converter = new OrientDbObjectConverter(dbSessionProvider);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>> Maybe<S> find(MetaClassWithKey<K, S, B> metaClass, K id) {
        return executeQuery(statement(
                fromClause(QueryInfo.<K, S, S, B>builder().metaClass(metaClass).build()),
                "where __key='" + id.toString() + "' limit 1"))
                .ofType(OElement.class)
                .map(el -> converter.toObject(el, metaClass.objectClass(), ImmutableList.copyOf(metaClass.properties())))
                .firstElement();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>> Single<List<S>> update(MetaClassWithKey<K, S, B> meta, Iterable<S> entities) {
        AtomicReference<ODatabaseSession> session = new AtomicReference<>();
        ensureClass(meta);
        return Observable
                .fromIterable(entities)
                .doOnLifecycle(
                        d -> {
                            session.set(databaseSession());
                            session.get().begin();
                            },
                        () -> session.getAndSet(null).commit())
                .flatMapSingle(entity -> createOrUpdateDocument(entity)
                        .compose(saveAndUpdateCache(entity))
                        .map(el -> converter.toObject(el, meta.objectClass(), ImmutableList.copyOf(entity.metaClass().properties()))))
                .toList();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>> Single<List<S>> insert(MetaClassWithKey<K, S, B> meta, Iterable<S> entities) {
        return update(meta, entities);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, T, Q extends
            HasEntityMeta<K, S, B>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasSortingInfo<S, B>
            & HasProperties<T>> Observable<T> query(Q statement) {
        return executeQuery(
                statement(
                        selectClause(statement),
                        fromClause(statement),
                        whereClause(statement),
                        limitClause(statement),
                        skipClause(statement)
                ))
                .ofType(OResult.class)
                .map(OResult::getElement)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(el -> converter.toObject(el, /*statement.mapping().objectType()*/(TypeToken<T>)null, statement.properties()));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, T, Q extends HasEntityMeta<K, S, B> & HasPredicate<S> & HasMapping<S, T> & HasPagination & HasSortingInfo<S, B> & HasProperties<T>> Observable<Notification<T>> liveQuery(Q query) {
        return null;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, T, R, Q extends HasEntityMeta<K, S, B> & HasPredicate<S> & HasMapping<S, T> & HasPagination & HasProperties<T>> Single<R> aggregate(Q query, Aggregator<S, T, R, ?> aggregator) {
        return null;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, T, R, Q extends HasEntityMeta<K, S, B> & HasPredicate<S> & HasMapping<S, T> & HasPagination & HasProperties<T>> Observable<R> liveAggregate(Q query, Aggregator<S, T, R, ?> aggregator) {
        return null;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, Q extends HasEntityMeta<K, S, B> & HasPredicate<S> & HasPropertyUpdates<S>> Observable<S> update(Q statement) {
        return executeQuery(statement(
                "update " + toClassName(statement.metaClass().objectClass())))
                .ofType(OElement.class)
                .map(element -> converter.toObject(element, statement.metaClass(), ImmutableList.copyOf(statement.metaClass().properties())));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, Q extends HasEntityMeta<K, S, B> & HasPredicate<S>> Completable delete(Q statement) {
        return executeCommand(statement(
                "delete",
                fromClause(statement),
                whereClause(statement)));
    }

    private String statement(String... clauses) {
        return Arrays.stream(clauses).filter(c -> !c.isEmpty()).collect(Collectors.joining("\n"));
    }

    private Observable<OResult> executeQuery(String queryStatement) {
        return toObservable(() -> {
            log.info("Querying: " + queryStatement);
            return databaseSession().query(queryStatement);
        });
    }

    private Completable executeCommand(String statement) {
        return Completable.create(emitter -> databaseSession().command(statement));
    }

    private <
            K,
            S extends HasMetaClassWithKey<K, S, B>,
            B extends BuilderPrototype<S, B>,
            T,
            Q extends HasMapping<S, T> & HasEntityMeta<K, S, B> & HasProperties<T>> String selectClause(Q statement) {
        return Optional
                .ofNullable(statement.mapping())
                .map(exp -> toMappingClause(exp, statement.properties()))
                .map(exp -> "select " + exp)
                .orElse("");
    }

    private <S, T> String toMappingClause(ObjectExpression<S, T> expression, Collection<PropertyMeta<T, ?, ?>> properies) {
        String exp = SqlExpressionGenerator.toSqlExpression(expression);
        return Optional.ofNullable(properies)
                .filter(p -> !p.isEmpty())
                .map(p -> p.stream()
                        .map(PropertyMeta::name)
                        .map(name -> exp + "." + name)
                        .collect(Collectors.joining(", ")))
                .orElse(exp);
    }

    private <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>, Q extends HasEntityMeta<K, S, B>> String fromClause(Q statement) {
        return "from " + toClassName(statement.metaClass().objectClass());
    }

    private <S, Q extends HasPredicate<S>> String whereClause(Q statement) {
        return Optional
                .ofNullable(statement.predicate())
                .map(this::toConditionClause)
                .map(cond -> "where " + cond)
                .orElse("");
    }

    private <Q extends HasPagination> String limitClause(Q statement) {
        return Optional.ofNullable(statement.limit())
                .map(count -> "limit " + count)
                .orElse("");
    }

    private <Q extends HasPagination> String skipClause(Q statement) {
        return Optional.ofNullable(statement.skip())
                .map(count -> "skip " + count)
                .orElse("");
    }

    private <S> String toConditionClause(ObjectExpression<S, Boolean> condition) {
        return SqlExpressionGenerator.toSqlExpression(condition);
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OClass ensureClass(MetaClass<T, TB> metaClass) {
        return classMap.computeIfAbsent(toClassName(metaClass.objectClass()), name -> createClass(metaClass));
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OClass createClass(MetaClass<T, TB> metaClass) {
        String className = toClassName(metaClass.objectClass());
        OClass oClass = databaseSession().createClassIfNotExist(className);
        Streams.fromIterable(metaClass.properties())
                .forEach(p -> addProperty(oClass, p));

        if (metaClass instanceof MetaClassWithKey) {
            if (!oClass.existsProperty("__key")) {
                oClass.createProperty("__key", OType.STRING);
                oClass.createIndex(className + ".__keyIndex", OClass.INDEX_TYPE.UNIQUE, "__key");
            }
        }

        String[] textFields = Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.type().asClass() == String.class)
                .map(PropertyMeta::name)
                .toArray(String[]::new);

        Streams.fromIterable(metaClass.properties())
                .filter(p -> p.type().is(HasMetaClassWithKey.class::isAssignableFrom))
                .map(PropertyMeta::type)
                .map(OrientDbQueryProvider::toMetaClass)
                .forEach(this::ensureClass);

        if (textFields.length > 0) {
            oClass.createIndex(className + ".textIndex", "FULLTEXT", null, null, "LUCENE", textFields);
        }

        return oClass;
    }

    private static <T extends HasMetaClass<T, B>, B extends BuilderPrototype<T, B>> MetaClass<T, B> toMetaClass(TypeToken typeToken) {
        //noinspection unchecked
        return MetaClasses.forToken((TypeToken<T>)typeToken);
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> void addProperty(OClass oClass, PropertyMeta<T, TB, ?> propertyMeta) {
        OType propertyOType = toOType(propertyMeta.type());
        if (propertyOType.isLink() || propertyOType.isEmbedded()) {
            OClass linkedOClass = databaseSession().getClass(toClassName(propertyMeta.type()));
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
                if (!Objects.equals(oProperty.getLinkedClass(), linkedOClass)) {
                    oProperty.setLinkedClass(linkedOClass);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType, linkedOClass);
            }
        } else {
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType);
            }
        }
    }

    static OType toOType(TypeToken<?> token) {
        Class<?> cls = token.asClass();
        return Optional
                .ofNullable(OType.getTypeByClass(cls))
                .orElseGet(() -> HasMetaClass.class.isAssignableFrom(cls)
                        ? (HasMetaClassWithKey.class.isAssignableFrom(cls) ? OType.LINK : OType.EMBEDDED)
                        : OType.ANY);
    }

    private static String toClassName(TypeToken<?> cls) {
        return toClassName(cls.asClass());
    }

    private static String toClassName(Class<?> cls) {
        return cls.getSimpleName();
    }

    private ODatabaseSession databaseSession() {
        return dbSessionProvider.get();
    }

    private static Observable<OResult> toObservable(Supplier<OResultSet> resultSetSupplier) {
        return Observable.create(emitter -> {
            OResultSet resultSet = resultSetSupplier.get();
            emitter.setCancellable(resultSet::close);
            resultSet.stream()
                    .peek(res -> log.info("Received: " + res))
                    .forEach(emitter::onNext);
            emitter.onComplete();
        });
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> Single<OElement> createDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;

        String className = toClassName(typedObj.metaClass().objectClass());
        MetaClass<T, TB> metaClass = typedObj.metaClass();
        ensureClass(metaClass);
        Map<String, Single<?>> values = Streams.fromIterable(metaClass.properties())
                .filter(prop -> prop.getValue(typedObj) != null)
                .collect(Collectors.toMap(PropertyMeta::name, prop -> toPropertyValue(prop, prop.getValue(typedObj))));

        OElement doc = databaseSession().newInstance(className);
        if (obj instanceof HasMetaClassWithKey) {
            //noinspection unchecked
            String key = toKey(((HasMetaClassWithKey)obj).metaClass().keyProperty().getValue(obj));
            doc.setProperty("__key", key);
        }

        return Observable.fromIterable(values.entrySet())
                .flatMapCompletable(entry -> entry.getValue().doOnSuccess(val -> doc.setProperty(entry.getKey(), val)).ignoreElement())
                .andThen(Single.just(doc));
    }

    private static <K> String toKey(K key) {
        return key.toString();
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> Single<?> toPropertyValue(PropertyMeta<T, TB, ?> propertyMeta, Object val) {
        OType oType = toOType(propertyMeta.type());
        if (oType.isEmbedded()) {
            if (!oType.isMultiValue()) {
                return Single.defer(() -> createDocument(val));
            } else {
                return Single.defer(() -> createDocumentCollection(val));
            }
        } else if (oType.isLink() && (val instanceof HasMetaClassWithKey)) {
//            HasMetaClassWithKey refVal = (HasMetaClassWithKey)val;
            return createOrUpdateDocument(val);
//                    toRecordId(refVal)
//                    .switchIfEmpty(Single
//                            .defer(() -> createDocument(val))
//                            .compose(saveAndUpdateCache(refVal))
//                            .map(ORecord::getIdentity));
        }
        return Single.just(val);
    }

    private Single<? extends Collection<OElement>> createDocumentCollection(Object val) {
        return Observable.fromIterable((Iterable<?>)val)
                .flatMapSingle(this::createDocument)
                .toList();
    }

    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> Single<OElement> createOrUpdateDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;
        return findDocument(typedObj)
                .flatMap(doc -> updateDocument(doc, typedObj).toMaybe())
                .switchIfEmpty(Single.defer(() -> createDocument(typedObj)));
    }

    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> Maybe<OElement> findDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;
        return Optional
                .ofNullable(typedObj)
                .map(Maybe::just)
                .orElseGet(Maybe::empty)
                .map(o -> o.metaClass().keyProperty().getValue(typedObj))
                .flatMap(key -> findDocument(typedObj.metaClass(), key));
    }

    private <K, S extends HasMetaClassWithKey<K, S, B>, B extends BuilderPrototype<S, B>> Maybe<OElement> findDocument(MetaClassWithKey<K, S, B> meta, K key) {
        ensureClass(meta);
        return executeQuery(statement(
                "select",
                fromClause(QueryInfo.<K, S, S, B>builder().metaClass(meta).build()),
                "where __key='" + key.toString() + "' limit 1"))
                .ofType(OElement.class)
                .firstElement();
    }

    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> Single<OElement> updateDocument(OElement doc, T obj) {
        return Observable
                .fromIterable(obj.metaClass().properties())
                .flatMapCompletable(prop -> Optional.ofNullable(prop.getValue(obj))
                            .map(val -> setPropertyValue(doc, prop, val))
                            .orElseGet(Completable::complete))
                .toSingle(() -> doc);
    }

    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> Completable setPropertyValue(OElement doc, PropertyMeta<T, TB, ?> propertyMeta, Object val) {
        OType oType = toOType(propertyMeta.type());
        Single<?> propVal = toPropertyValue(propertyMeta, val);
        return propVal.map(v -> {
            doc.setProperty(propertyMeta.name(), v, oType);
            return doc;
        }).ignoreElement();
    }

    private Maybe<ORecordId> loadId(KeyInfo key) {
        return executeQuery(statement(
                "select @rid",
                "from " + key.className(),
                "where __key='" + key.key() + "'"))
                .firstElement()
                .doOnSuccess(el -> log.info("Record id retrieved: {}" + el))
                .<ORecordId>map(el -> el.getProperty("@rid"))
                .doOnSuccess(rid -> log.info("Record id retrieved: {}" + rid))
                .cache();
    }

    private <K, T extends HasMetaClassWithKey<K, T, B>, B extends BuilderPrototype<T, B>> Maybe<ORecordId> toRecordId(MetaClassWithKey<K, T, B> metaClass, K key) {
        ensureClass(metaClass);
        try {
            return keyCache.get(KeyInfo.create(metaClass, key));
        } catch (ExecutionException e) {
            return Maybe.error(e);
        }
    }

    private <K, T extends HasMetaClassWithKey<K, T, B>, B extends BuilderPrototype<T, B>> Maybe<ORecordId> toRecordId(HasMetaClassWithKey<K, T, B> obj) {
        //noinspection unchecked
        return toRecordId(obj.metaClass(), obj.metaClass().keyProperty().getValue((T)obj));
    }

    private SingleTransformer<OElement, OElement> saveAndUpdateCache(HasMetaClassWithKey<?, ?, ?> obj) {
        return el -> el.<OElement>map(OElement::save).doOnSuccess(d -> {
            ORecordId rid = d.getProperty("@rid");
            keyCache.put(KeyInfo.fromObject(obj), Maybe.just(rid));
            log.info("Saved document: " + d);
        });
    }
}
