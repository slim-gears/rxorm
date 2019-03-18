package com.slimgears.rxorm.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.Aggregator;
import com.slimgears.util.repository.expressions.CollectionExpression;
import com.slimgears.util.repository.expressions.ConstantExpression;
import com.slimgears.util.repository.expressions.ObjectExpression;
import com.slimgears.util.repository.expressions.PropertyExpression;
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
import com.slimgears.util.repository.util.PropertyResolver;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleTransformer;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OrientDbQueryProvider implements QueryProvider {
    private final static String aggregationField = "__aggregation";
    private final static Logger log = Logger.getLogger(OrientDbQueryProvider.class.getName());
    private final ThreadLocal<ODatabaseSession> dbSessionProvider;
    private final Map<String, OClass> classMap = new HashMap<>();

    @Inject
    public OrientDbQueryProvider(Provider<ODatabaseSession> dbSessionProvider) {
        this.dbSessionProvider = ThreadLocal.withInitial(dbSessionProvider::get);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> find(MetaClassWithKey<K, S> metaClass, K id) {
        return executeQuery(statement(
                fromClause(QueryInfo.<K, S, S>builder().metaClass(metaClass).build()),
                "where " + metaClass.keyProperty().name() + "=" + SqlExpressionGenerator.toSqlExpression(ConstantExpression.of(id)) + " limit 1"))
                .ofType(OResult.class)
                .map(res -> OResultPropertyResolver.create(databaseSession(), res).toObject(metaClass))
                .firstElement();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<List<S>> update(MetaClassWithKey<K, S> meta, Iterable<S> entities) {
        ensureClass(meta);
        return Observable
                .fromIterable(entities)
                //.doOnLifecycle(d -> databaseSession().begin(), () -> databaseSession().commit())
                .flatMapSingle(entity -> createOrUpdateDocument(entity)
                        .map(OElement::<OElement>save)
                        .map(el -> OElementPropertyResolver.create(databaseSession(), el).toObject(meta)))
                .retry(4)
                .toList();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Single<List<S>> insert(MetaClassWithKey<K, S> meta, Iterable<S> entities) {
        return update(meta, entities);
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasSortingInfo<S>
            & HasProperties<T>> Observable<T> query(Q query) {
        ensureClass(query.metaClass());
        TypeToken<? extends T> objectType = getObjectType(query);

        return executeQuery(
                statement(
                        selectClause(query),
                        fromClause(query),
                        whereClause(query),
                        limitClause(query),
                        skipClause(query)
                ))
                .ofType(OResult.class)
                .map(res -> OResultPropertyResolver.create(databaseSession(), res).toObject(objectType));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasSortingInfo<S>
            & HasProperties<T>> Observable<Notification<T>> liveQuery(Q query) {
        ensureClass(query.metaClass());
        TypeToken<? extends T> objectType = getObjectType(query);

        return executeLiveQuery(
                statement(
                        "live",
                        selectClause(query),
                        fromClause(query),
                        whereClause(query),
                        limitClause(query),
                        skipClause(query)))
                .map(res -> Notification.ofModified(
                        Optional.ofNullable(res.oldResult())
                                .map(or -> OResultPropertyResolver.create(databaseSession(), or))
                                .map(pr -> pr.toObject(objectType))
                                .orElse(null),
                        Optional.ofNullable(res.newResult())
                                .map(or -> OResultPropertyResolver.create(databaseSession(), or))
                                .map(pr -> pr.toObject(objectType))
                                .orElse(null)));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasProperties<T>> Single<R> aggregate(Q query, Aggregator<T, T, R, ?> aggregator) {

        ensureClass(query.metaClass());
        TypeToken<? extends T> elementType = getObjectType(query);
        ObjectExpression<T, R> expression = aggregator.apply(CollectionExpression.indirectArg(elementType));

        return executeQuery(statement(
                "select " + SqlExpressionGenerator.toSqlExpression(expression, query.mapping()) + " as " + aggregationField,
                fromClause(query),
                whereClause(query),
                limitClause(query),
                skipClause(query)))
                .singleOrError()
                .map(res -> {
                    Object obj = AbstractOrientPropertyResolver.toValue(databaseSession(), res.getProperty(aggregationField), expression.objectType().asClass());
                    //noinspection unchecked
                    return (obj instanceof PropertyResolver)
                            ? ((PropertyResolver)obj).toObject(expression.objectType())
                            : (R)obj;
                });
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T, R, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasMapping<S, T>
            & HasPagination
            & HasProperties<T>> Observable<R> liveAggregate(Q query, Aggregator<T, T, R, ?> aggregator) {

        ensureClass(query.metaClass());
        TypeToken<? extends T> elementType = getObjectType(query);
        ObjectExpression<T, R> expression = aggregator.apply(CollectionExpression.indirectArg(elementType));

        return executeLiveQuery(statement(
                "live select " + SqlExpressionGenerator.toSqlExpression(expression, query.mapping()) + " as " + aggregationField,
                fromClause(query),
                whereClause(query),
                limitClause(query),
                skipClause(query)))
                .flatMapSingle(res -> aggregate(query, aggregator))
                .distinctUntilChanged();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, Q extends
            HasEntityMeta<K, S>
            & HasPredicate<S>
            & HasPropertyUpdates<S>> Observable<S> update(Q statement) {
        ensureClass(statement.metaClass());
        return executeQuery(statement(
                "update " + toClassName(statement.metaClass().objectClass()),
                "set " + statement.propertyUpdates()
                        .stream()
                        .map(pu -> statement(SqlExpressionGenerator.toSqlExpression(pu.property()), " = ", SqlExpressionGenerator.toSqlExpression(pu.updater())))
                        .collect(Collectors.joining(", "))))
                .ofType(OElement.class)
                .map(el -> OElementPropertyResolver.create(databaseSession(), el).toObject(statement.metaClass()));
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, Q extends HasEntityMeta<K, S> & HasPredicate<S>> Completable delete(Q statement) {
        return executeCommand(statement(
                "delete",
                fromClause(statement),
                whereClause(statement)))
                .retry(4);
    }

    private String statement(String... clauses) {
        return Arrays
                .stream(clauses).filter(c -> !c.isEmpty())
                .collect(Collectors.joining(" "));
    }

    private Observable<OResult> executeQuery(String queryStatement) {
        return toObservable(() -> {
            log.fine(() -> "Querying: " + queryStatement);
            return databaseSession().query(queryStatement);
        });
    }

    private Observable<OrientDbLiveQueryListener.LiveQueryNotification> executeLiveQuery(String queryStatement) {
        return Observable
                .<OrientDbLiveQueryListener.LiveQueryNotification>create(emitter -> {
                    log.fine(() -> "Live querying: " + queryStatement);
                    OLiveQueryMonitor monitor = databaseSession().live(queryStatement, new OrientDbLiveQueryListener(emitter));
                    emitter.setCancellable(monitor::unSubscribe);
                })
                .doOnNext(n -> log.fine(() -> "Notification received: " + n));
    }


    private Completable executeCommand(String statement) {
        return Completable.create(emitter -> {
            log.fine(() -> "Executing command: " + statement);
            databaseSession().command(statement);
            emitter.onComplete();
        });
    }

    private <
            K,
            S extends HasMetaClassWithKey<K, S>,
            T,
            Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q statement) {
        return Optional
                .ofNullable(statement.mapping())
                .map(exp -> toMappingClause(exp, statement.properties()))
                .filter(exp -> !exp.isEmpty())
                .map(exp -> "select " + exp)
                .orElse("select");
    }

    private <S, T> String toMappingClause(ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properies) {
        String exp = Optional
                .of(SqlExpressionGenerator.toSqlExpression(expression))
                .filter(e -> !e.isEmpty())
                .map(e -> e + ".")
                .orElse("");

        return Optional.ofNullable(properies)
                .filter(p -> !p.isEmpty())
                .map(p -> p.stream()
                        .map(SqlExpressionGenerator::toSqlExpression)
                        .collect(Collectors.joining(", ")))
                .orElse(exp);
    }

    private <K, S extends HasMetaClassWithKey<K, S>, Q extends HasEntityMeta<K, S>> String fromClause(Q statement) {
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

    private <T extends HasMetaClass<T>> OClass ensureClass(MetaClass<T> metaClass) {
        return classMap.computeIfAbsent(toClassName(metaClass.objectClass()), name -> createClass(metaClass));
    }

    private <T extends HasMetaClass<T>> OClass createClass(MetaClass<T> metaClass) {
        String className = toClassName(metaClass.objectClass());
        OClass oClass = databaseSession().createClassIfNotExist(className);
        Streams.fromIterable(metaClass.properties())
                .forEach(p -> addProperty(oClass, p));

        if (metaClass instanceof MetaClassWithKey) {
            MetaClassWithKey metaClassWithKey = (MetaClassWithKey) metaClass;
            PropertyMeta keyProperty = metaClassWithKey.keyProperty();
            if (!oClass.areIndexed(keyProperty.name())) {
                oClass.createIndex(className + "." + keyProperty.name() + "Index", OClass.INDEX_TYPE.UNIQUE, keyProperty.name());
            }
        }

//        String[] textFields = Streams
//                .fromIterable(metaClass.properties())
//                .filter(p -> p.type().asClass() == String.class)
//                .map(PropertyMeta::name)
//                .toArray(String[]::new);

        Streams.fromIterable(metaClass.properties())
                .filter(p -> p.type().is(HasMetaClassWithKey.class::isAssignableFrom))
                .map(PropertyMeta::type)
                .map(OrientDbQueryProvider::toMetaClass)
                .forEach(this::ensureClass);

//        if (textFields.length > 0) {
//            oClass.createIndex(className + ".textIndex", "FULLTEXT", null, null, "LUCENE", textFields);
//        }

        return oClass;
    }

    private static <T extends HasMetaClass<T>> MetaClass<T> toMetaClass(TypeToken typeToken) {
        //noinspection unchecked
        return MetaClasses.forToken((TypeToken<T>)typeToken);
    }

    private <T extends HasMetaClass<T>> void addProperty(OClass oClass, PropertyMeta<T, ?> propertyMeta) {
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
        ODatabaseSession databaseSession = dbSessionProvider.get();
        databaseSession.activateOnCurrentThread();
        return databaseSession;
    }

    private static Observable<OResult> toObservable(Supplier<OResultSet> resultSetSupplier) {
        return Observable.create(emitter -> {
            OResultSet resultSet = resultSetSupplier.get();
            emitter.setCancellable(resultSet::close);
            resultSet.stream()
                    .peek(res -> log.fine(() -> "Received: " + res))
                    .forEach(emitter::onNext);
            emitter.onComplete();
        });
    }

    private <T extends HasMetaClass<T>> Single<OElement> createDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;

        String className = toClassName(typedObj.metaClass().objectClass());
        MetaClass<T> metaClass = typedObj.metaClass();
        ensureClass(metaClass);
        Map<String, Single<?>> values = Streams.fromIterable(metaClass.properties())
                .filter(prop -> prop.getValue(typedObj) != null)
                .collect(Collectors.toMap(PropertyMeta::name, prop -> toPropertyValue(prop, prop.getValue(typedObj))));

        OElement doc = databaseSession().newInstance(className);

        return Observable.fromIterable(values.entrySet())
                .flatMapCompletable(entry -> entry.getValue().doOnSuccess(val -> {
                    log.finest(() -> "Setting property " + entry.getKey() + ": " + val + "(" + (val != null ? val.getClass() : "null") + ")");
                    doc.setProperty(entry.getKey(), val);
                }).ignoreElement())
                .andThen(Single.just(doc));
    }

    private static <K> String toKey(K key) {
        return key.toString();
    }

    private <T extends HasMetaClass<T>> Single<?> toPropertyValue(PropertyMeta<T, ?> propertyMeta, Object val) {
        OType oType = toOType(propertyMeta.type());
        if (oType.isEmbedded()) {
            if (!oType.isMultiValue()) {
                return Single.defer(() -> createDocument(val));
            } else {
                return Single.defer(() -> createDocumentCollection(val));
            }
        } else if (oType.isLink() && (val instanceof HasMetaClassWithKey)) {
//            HasMetaClassWithKey refVal = (HasMetaClassWithKey)val;
            return findOrCreateDocument(val);
//            return
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

    private <K, T extends HasMetaClassWithKey<K, T>> Single<OElement> createOrUpdateDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;
        return findDocument(typedObj)
                .flatMap(doc -> {
                    log.finest(() -> "Document for " + doc + " found. Updating...");
                    return this.updateDocument(doc, typedObj).<OElement>map(OElement::save).toMaybe();
                })
                .switchIfEmpty(Single.defer(() -> {
                    log.finest(() -> "Document for " + obj + " not found. Creating...");
                    Single<OElement> element = createDocument(typedObj).map(OElement::save);
                    return element.doOnSuccess(doc -> log.fine(() -> "Created document for " + obj + ": " + doc));
                }));
    }

    private <K, T extends HasMetaClassWithKey<K, T>> Single<OElement> findOrCreateDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;
        return findDocument(typedObj)
                .switchIfEmpty(Single.defer(() -> {
                    log.finest(() -> "Document for " + obj + " not found. Creating...");
                    Single<OElement> element = createDocument(typedObj);
                    return element.doOnSuccess(doc -> log.fine(() -> "Created document for " + obj + ": " + doc));
                }));
    }

    private <K, T extends HasMetaClassWithKey<K, T>> Maybe<OElement> findDocument(Object obj) {
        //noinspection unchecked
        T typedObj = (T)obj;
        return Optional
                .ofNullable(typedObj)
                .map(Maybe::just)
                .orElseGet(Maybe::empty)
                .map(o -> o.metaClass().keyProperty().getValue(typedObj))
                .flatMap(key -> findDocument(typedObj.metaClass(), key));
    }

    private <K, S extends HasMetaClassWithKey<K, S>> Maybe<OElement> findDocument(MetaClassWithKey<K, S> meta, K key) {
        ensureClass(meta);
        return executeQuery(statement(
                "select",
                fromClause(QueryInfo.<K, S, S>builder().metaClass(meta).build()),
                "where " + meta.keyProperty().name() + "=" + SqlExpressionGenerator.toSqlExpression(ConstantExpression.of(key)) + " limit 1"))
                .ofType(OResult.class)
                .map(OResult::toElement)
                .firstElement();
    }

    private <K, T extends HasMetaClassWithKey<K, T>> Single<OElement> updateDocument(OElement doc, T obj) {
        return Observable
                .fromIterable(obj.metaClass().properties())
                .flatMapCompletable(prop -> Optional.ofNullable(prop.getValue(obj))
                            .map(val -> setPropertyValue(doc, prop, val))
                            .orElseGet(Completable::complete))
                .toSingle(() -> doc);
    }

    private <T extends HasMetaClass<T>> Completable setPropertyValue(OElement doc, PropertyMeta<T, ?> propertyMeta, Object val) {
        OType oType = toOType(propertyMeta.type());
        Single<?> propVal = toPropertyValue(propertyMeta, val);
        return propVal.map(v -> {
            doc.setProperty(propertyMeta.name(), v, oType);
            return doc;
        }).ignoreElement();
    }

    private SingleTransformer<OElement, OElement> save() {
        return el -> el.map(OElement::save);
    }

    private <S extends HasMetaClassWithKey<?, S>, T, Q extends HasMapping<S, T> & HasEntityMeta<?, S>> TypeToken<? extends T> getObjectType(Q query) {
        //noinspection unchecked
        return Optional
                .ofNullable(query.mapping())
                .map(ObjectExpression::objectType)
                .orElseGet(() -> (TypeToken)query.metaClass().objectClass());
    }
}
