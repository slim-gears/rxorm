//package com.slimgears.rxorm.orientdb;
//
//import com.orientechnologies.orient.core.metadata.schema.OClass;
//import com.orientechnologies.orient.core.sql.executor.OResult;
//import com.slimgears.util.reflect.TypeToken;
//import com.slimgears.util.repository.query.QueryProvider;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class OrientDbRepository {
//    private final Map<TypeToken, OClass> registeredClasses = new ConcurrentHashMap<>();
//    private final long bufferTimeMillis;
//
//    static class OnCreateLiveQueryNotification extends LiveQueryNotification {
//        OnCreateLiveQueryNotification(OResult result) {
//            super(result);
//        }
//    }
//
//    static class OnDeleteLiveQueryNotification extends LiveQueryNotification {
//        OnDeleteLiveQueryNotification(OResult result) {
//            super(result);
//        }
//    }
//
//    private final Provider<ODatabaseSession> sessionProvider;
//
//    @Inject
//    public OrientDbRepository(@RepositoryDb Provider<ODatabaseSession> sessionProvider,
//                              @Named("backend.repository.orientdb.bufferTimeMillis") long bufferTimeMillis) {
//        log.info("{} created", getClass().getSimpleName());
//        this.bufferTimeMillis = bufferTimeMillis;
//        this.sessionProvider = sessionProvider;
//    }
//
//    private class InternalEntityCollection<
//            K,
//            T extends HasMetaClassWithKey<K, T, TB>,
//            TB extends BuilderPrototype<T, TB>,
//            F extends HasMetaClass<F, ?>> implements EntityCollection<K, T, TB, F> {
//        private final MetaClassWithKey<K, T, TB> entityMeta;
//
//        private InternalEntityCollection(MetaClassWithKey<K, T, TB> entityMeta) {
//            this.entityMeta = entityMeta;
//            ensureClassRegistered(entityMeta);
//        }
//
//        @Override
//        public MetaClassWithKey<K, T, TB> metaClass() {
//            return entityMeta;
//        }
//
//        @Override
//        public Observable<Long> observeCount(F filter) {
//            String query = Formatter.of("select count (*) from ${class} ${condition}")
//                    .value("class", toClassName(entityMeta.objectClass()))
//                    .value("condition", filterToCondition(entityMeta, filter))
//                    .toString();
//
//            AtomicLong lastCount = new AtomicLong();
//
//            return OrientDbRepository.this.query(query)
//                    .map(r -> r.<Long>getProperty("count(*)"))
//                    .doOnNext(lastCount::set)
//                    .concatWith(liveQuery(query)
//                            .flatMapMaybe(notification -> {
//                                if (notification instanceof OnCreateLiveQueryNotification) {
//                                    return Maybe.just(lastCount.incrementAndGet());
//                                } else if (notification instanceof OnDeleteLiveQueryNotification) {
//                                    return Maybe.just(lastCount.decrementAndGet());
//                                } else {
//                                    return Maybe.empty();
//                                }
//                            }));
//        }
//
//        @Override
//        public Observable<CollectionNotification<T>> observe(DataQuery<F> query) {
//            String baseQuery = Formatter
//                    .of("select from ${class} ${condition}")
//                    .value("class", toClassName(entityMeta.objectClass()))
//                    .value("condition", filterToCondition(entityMeta, Optional.ofNullable(query).map(DataQuery::filter).orElse(null)))
//                    .toString();
//
//            return Observable.just(query(query))
//                    .compose(OrientDbRepository.this.<T, TB>collectionNotificationFromObjects())
//                    .concatWith(liveQuery(baseQuery).compose(OrientDbRepository.this.<T, TB>collectionNotificationFromLiveQuery(entityMeta)))
//                    .doOnError(e -> log.error("Error occurred when processing notification", e));
//        }
//
//        @Override
//        public Single<Long> queryCount(F filter) {
//            String query = Formatter.of("select count (*) from ${class} ${condition}")
//                    .value("class", toClassName(entityMeta.objectClass()))
//                    .value("condition", filterToCondition(entityMeta, filter))
//                    .toString();
//
//            return Single.defer(() -> OrientDbRepository.this.query(query)
//                    .map(element -> element.<Long>getProperty("count(*)"))
//                    .first(0L));
//        }
//
//        @Override
//        public Observable<T> query(DataQuery<F> query) {
//            String queryString = Formatter
//                    .of("select from ${class} ${condition} ${limit} ${skip}")
//                    .value("class", toClassName(entityMeta.objectClass()))
//                    .value("condition", filterToCondition(entityMeta, Optional.ofNullable(query).map(DataQuery::filter).orElse(null)))
//                    .value("limit", Optional.ofNullable(query).map(DataQuery::limit).map(l -> "limit " + l).orElse(""))
//                    .value("skip", Optional.ofNullable(query).map(DataQuery::skip).map(s -> "skip " + s).orElse(""))
//                    .toString();
//
//            return OrientDbRepository.this.query(queryString).map(e -> toObject(entityMeta, e.toElement()));
//        }
//
//        @Override
//        public Single<T> update(K key, Function<Maybe<T>, Single<T>> update) {
//            return findByKey(key)
//                    .flatMap(item -> update.apply(Maybe.just(item)).toMaybe())
//                    .switchIfEmpty(Single.defer(() -> update.apply(Maybe.empty())))
//                    .map(item -> OrientDbRepository.this.createOrUpdateDocument(item).<OElement>save())
//                    .doOnError(error -> log.error("Error occurred when updating document {}", error))
//                    .map(doc -> toObject(entityMeta, doc))
//                    .compose(Singles.backoffDelayRetry(Duration.ofMillis(100), 10))
//                    .compose(Singles.startNow());
//        }
//
//        @Override
//        public Completable delete(Observable<T> objects) {
//            return objects
//                    .map(OrientDbRepository.this::findDocument)
//                    .filter(Optional::isPresent)
//                    .map(Optional::get)
//                    .doOnNext(OElement::delete)
//                    .ignoreElements()
//                    .compose(Completables.startNow());
//        }
//
//        @Override
//        public Maybe<T> findByKey(K key) {
//            return findDocument(entityMeta, key)
//                    .map(element -> toObject(entityMeta, element));
//        }
//
//        @Override
//        public Completable deleteByKey(K key) {
//            return findDocument(entityMeta, key)
//                    .map(ORecord::delete)
//                    .ignoreElement()
//                    .compose(Completables.startNow());
//        }
//    }
//
//    private <F extends HasMetaClass<F, ?>> String filterToCondition(MetaClass<?, ?> metaClass, F filter) {
//        StringBuilder conditionBuilder = new StringBuilder();
//        if (hasTextFilter(filter)) {
//            conditionBuilder.append(Formatter
//                    .of("search_index(\"${class}.textIndex\", \"${wildcard}\") = true")
//                    .value("class", toClassName(metaClass.objectClass()))
//                    .value("wildcard", toWildcard((HasTextQuery)filter))
//                    .toString());
//        }
//        return Optional.of(conditionBuilder.toString())
//                .filter(str -> !str.isEmpty())
//                .map(str -> "where " + str)
//                .orElse("");
//    }
//
//    private <F> boolean hasTextFilter(F filter) {
//        return filter instanceof HasTextQuery && !Strings.isNullOrEmpty(((HasTextQuery) filter).textFilter());
//    }
//
//    private String toWildcard(HasTextQuery query) {
//        return String.join("*", Objects.requireNonNull(query.textFilter()).split("\\s")) + '*';
//    }
//
//    private Observable<OResult> query(String query) {
//        return toObservable(sessionProvider.get().query(query));
//    }
//
//    static class ResultListener implements OLiveQueryResultListener {
//        private final ObservableEmitter<LiveQueryNotification> emitter;
//
//        ResultListener(ObservableEmitter<LiveQueryNotification> emitter) {
//            this.emitter = emitter;
//        }
//
//        @Override
//        public void onCreate(ODatabaseDocument database, OResult data) {
//            emitter.onNext(new OnCreateLiveQueryNotification(data));
//        }
//
//        @Override
//        public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
//            emitter.onNext(new OnUpdateLiveQueryNotification(before, after));
//        }
//
//        @Override
//        public void onDelete(ODatabaseDocument database, OResult data) {
//            emitter.onNext(new OnDeleteLiveQueryNotification(data));
//        }
//
//        @Override
//        public void onError(ODatabaseDocument database, OException exception) {
//            emitter.onError(exception);
//        }
//
//        @Override
//        public void onEnd(ODatabaseDocument database) {
//            emitter.onComplete();
//        }
//    }
//
//    private Observable<LiveQueryNotification> liveQuery(String query) {
//        return Observable.<LiveQueryNotification>create(emitter -> {
//            ODatabaseSession session = sessionProvider.get();
//            OLiveQueryMonitor monitor = session.live(query, new ResultListener(emitter));
//            emitter.setCancellable(monitor::unSubscribe);
//        }).subscribeOn(Schedulers.io());
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OElement createDocument(Object obj) {
//        //noinspection unchecked
//        T typedObj = (T)obj;
//
//        String className = toClassName(typedObj.metaClass().objectClass());
//        MetaClass<T, TB> metaClass = typedObj.metaClass();
//        ensureClassRegistered(metaClass);
//        Map<String, Object> values = Streams.fromIterable(metaClass.properties())
//                .filter(prop -> prop.getValue(typedObj) != null)
//                .collect(Collectors.toMap(PropertyMeta::name, prop -> toPropertyValue(prop, prop.getValue(typedObj))));
//
//        OElement doc = sessionProvider.get().newInstance(className);
//        values.forEach(doc::setProperty);
//
//        if (obj instanceof HasMetaClassWithKey) {
//            //noinspection unchecked
//            doc.setProperty("__key", toKey(((HasMetaClassWithKey)obj).metaClass().keyProperty().getValue(obj)));
//        }
//
//        return doc;
//    }
//
//    private <K> String toKey(K key) {
//        return key.toString();
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> void ensureClassRegistered(MetaClass<T, TB> metaClass) {
//        registeredClasses.computeIfAbsent(metaClass.objectClass(), t -> createClassIfNotExists(metaClass));
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OElement setPropertyValue(OElement doc, PropertyMeta<T, TB, ?> propertyMeta, Object val) {
//        OType oType = toOType(propertyMeta.type());
//        Object propVal = toPropertyValue(propertyMeta, val);
//        doc.setProperty(propertyMeta.name(), propVal, oType);
//        return doc;
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> Object toPropertyValue(PropertyMeta<T, TB, ?> propertyMeta, Object val) {
//        OType oType = toOType(propertyMeta.type());
//        if (oType.isEmbedded()) {
//            if (!oType.isMultiValue()) {
//                return createDocument(val);
//            } else {
//                return createDocumentCollection(val);
//            }
//        } else if (oType.isLink()) {
//            OElement oElement = createOrUpdateDocument(val);
//            return oElement.save();
//        }
//        return val;
//    }
//
//    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> OElement createOrUpdateDocument(Object obj) {
//        //noinspection unchecked
//        T typedObj = (T)obj;
//        return findDocument(typedObj)
//                .map(doc -> updateDocument(doc, typedObj))
//                .orElseGet(() -> createDocument(typedObj));
//    }
//
//    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> OElement updateDocument(OElement doc, T obj) {
//        Streams
//                .fromIterable(obj.metaClass().properties())
//                .forEach(p -> {
//                    //noinspection unchecked
//                    Optional.ofNullable(((PropertyMeta)p).getValue(obj))
//                            .ifPresent(val -> setPropertyValue(doc, p, val));
//                });
//        return doc;
//    }
//
//    private Collection<OElement> createDocumentCollection(Object val) {
//        return Streams.fromIterable((Iterable<?>)val)
//                .map(this::createDocument)
//                .collect(Collectors.toList());
//    }
//
//    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> Maybe<OElement> findDocument(MetaClassWithKey<K, T, TB> meta, K key) {
//        ensureClassRegistered(meta);
//        String className = toClassName(meta.objectClass());
//        return query(
//                Formatter.of("select from ${class} where __key = '${key}'")
//                        .value("class", className)
//                        .value("key", toKey(key))
//                        .toString())
//                .map(OResult::toElement)
//                .firstElement();
//    }
//
//    private <K, T extends HasMetaClassWithKey<K, T, TB>, TB extends BuilderPrototype<T, TB>> Optional<OElement> findDocument(Object obj) {
//        //noinspection unchecked
//        T typedObj = (T)obj;
//        return Optional
//                .ofNullable(typedObj)
//                .map(o -> o.metaClass().keyProperty().getValue(typedObj))
//                .flatMap(key -> findDocument(typedObj.metaClass(), key)
//                        .map(Optional::of)
//                        .defaultIfEmpty(Optional.empty())
//                        .blockingGet());
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OClass createClassIfNotExists(MetaClass<T, TB> metaClass) {
//        return Optional.ofNullable(sessionProvider.get().getClass(toClassName(metaClass.objectClass())))
//                .orElseGet(() -> createClass(metaClass));
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> OClass createClass(MetaClass<T, TB> metaClass) {
//        ODatabaseSession session = sessionProvider.get();
//        String className = toClassName(metaClass.objectClass());
//        OClass oClass = session.createClass(className);
//        metaClass.properties().forEach(p -> addProperty(oClass, p));
//
//        if (metaClass instanceof MetaClassWithKey) {
//            oClass.createProperty("__key", OType.STRING);
//            oClass.createIndex(className + ".__keyIndex", OClass.INDEX_TYPE.UNIQUE, "__key");
//        }
//
//        String[] textFields = Streams
//                .fromIterable(metaClass.properties())
//                .filter(p -> p.type().asClass() == String.class)
//                .map(PropertyMeta::name)
//                .toArray(String[]::new);
//
//        if (textFields.length > 0) {
//            oClass.createIndex(className + ".textIndex", "FULLTEXT", null, null, "LUCENE", textFields);
//        }
//
//        return oClass;
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> void addProperty(OClass oClass, PropertyMeta<T, TB, ?> propertyMeta) {
//        if (oClass.existsProperty(propertyMeta.name())) {
//            return;
//        }
//
//        OType propertyOType = toOType(propertyMeta.type());
//        if (propertyOType.isLink() || propertyOType.isEmbedded()) {
//            OClass linkedOClass = sessionProvider.get().getClass(toClassName(propertyMeta.type()));
//            oClass.createProperty(propertyMeta.name(), propertyOType, linkedOClass);
//        } else {
//            oClass.createProperty(propertyMeta.name(), propertyOType);
//        }
//    }
//
//    private OType toOType(TypeToken<?> token) {
//        Class<?> cls = token.asClass();
//        return Optional
//                .ofNullable(OType.getTypeByClass(cls))
//                .orElseGet(() -> HasMetaClass.class.isAssignableFrom(cls)
//                        ? (HasMetaClassWithKey.class.isAssignableFrom(cls) ? OType.LINK : OType.EMBEDDED)
//                        : OType.ANY);
//    }
//
//    private String toClassName(TypeToken<?> cls) {
//        return toClassName(cls.asClass());
//    }
//
//    private String toClassName(Class<?> cls) {
//        return cls.getSimpleName();
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> ObservableTransformer<Observable<T>, CollectionNotification<T>> collectionNotificationFromObjects() {
//        return src -> src
//                .flatMapSingle(items -> items.toList()
//                        .map(lst ->
//                                CollectionNotification.<T>builder().added(ImmutableList.copyOf(lst)).build()));
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> ObservableTransformer<LiveQueryNotification, CollectionNotification<T>> collectionNotificationFromLiveQuery(MetaClass<T, TB> metaClass) {
//        return src -> src
//                .buffer(bufferTimeMillis, TimeUnit.MILLISECONDS)
//                .map(notifications -> {
//                    CollectionNotification.Builder<T> builder = CollectionNotification.builder();
//                    notifications.forEach(notification -> {
//                        if (notification instanceof OnCreateLiveQueryNotification) {
//                            builder.added(ImmutableList.of(toObject(metaClass, notification.result.toElement())));
//                        } else if (notification instanceof OnUpdateLiveQueryNotification) {
//                            builder.modified(ImmutableList.of(
//                                    EntityChange.<T>builder()
//                                            .oldValue(toObject(metaClass, ((OnUpdateLiveQueryNotification) notification).before().toElement()))
//                                            .newValue(toObject(metaClass, ((OnUpdateLiveQueryNotification) notification).after().toElement()))
//                                            .build()));
//                        } else if (notification instanceof OnDeleteLiveQueryNotification) {
//                            builder.deleted(ImmutableList.of(toObject(metaClass, notification.result.toElement())));
//                        }
//                    });
//                    return builder.build();
//                });
//    }
//
//    private <T extends HasMetaClass<T, TB>, TB extends BuilderPrototype<T, TB>> T toObject(MetaClass<T, TB> metaClass, OElement document) {
//        return toObject(metaClass.objectClass(), document);
//    }
//
//    private <T> T toObject(TypeToken<?> type, OElement element) {
//        if (element == null) {
//            return null;
//        }
//
//        //noinspection unchecked
//        MetaClass metaClass = MetaClasses.forClass((Class)type.asClass());
//        BuilderPrototype builder = metaClass.createBuilder();
//        //noinspection unchecked
//        Streams.fromIterable((Iterable<PropertyMeta>)metaClass.properties())
//                .forEach(p -> {
//                    OType otype = toOType(p.type());
//                    Object val = element.getProperty(p.name());
//                    if (val == null) {
//                        p.setValue(builder, null);
//                        return;
//                    }
//
//                    if (!otype.isLink() && !otype.isEmbedded()) {
//                        if (val != null && p.type().asClass().isEnum()) {
//                            //noinspection unchecked
//                            val = Enum.valueOf(p.type().asClass(), val.toString());
//                        }
//                        //noinspection unchecked
//                        p.setValue(builder, val);
//                    } else {
//                        if (otype.isMultiValue()) {
//                            if (val instanceof OTrackedList) {
//                                //noinspection unchecked
//                                p.setValue(builder, ((OTrackedList)val)
//                                        .stream()
//                                        .map(el -> toObject(typeParam(p.type()), (OElement)el))
//                                        .collect(ImmutableList.toImmutableList()));
//                            } else if (val instanceof OTrackedMap) {
//                                //noinspection unchecked
//                                p.setValue(builder, ((OTrackedMap)val)
//                                        .entrySet()
//                                        .stream()
//                                        .collect(ImmutableMap.<Map.Entry<String, Object>, String, Object>toImmutableMap(
//                                                Map.Entry::getKey,
//                                                entry -> toObject(typeParam(p.type()), (OElement)entry.getValue()))));
//                            } else if (val instanceof OTrackedSet) {
//                                //noinspection unchecked
//                                p.setValue(builder, ((OTrackedSet)val)
//                                        .stream()
//                                        .map(el -> toObject(typeParam(p.type()), (OElement)el))
//                                        .collect(ImmutableSet.toImmutableSet()));
//                            } else if (val instanceof List){
//                                //noinspection unchecked
//                                p.setValue(builder, ImmutableList.copyOf((List)val));
//                            } else if (val instanceof Set) {
//                                p.setValue(builder, ImmutableSet.copyOf((Set)val));
//                            } else if (val instanceof Map) {
//                                p.setValue(builder, ImmutableMap.copyOf((Map)val));
//                            } else {
//                                log.error("Not supported value type: {}" , val.getClass().getName());
//                            }
//                        } else {
//                            if (val instanceof OElement) {
//                                p.setValue(builder, toObject(p.type(), (OElement)val));
//                            } else if (val instanceof ORecordId) {
//                                p.setValue(builder, toObject(p.type(), sessionProvider.get().getRecord((ORecordId)val)));
//                            } else {
//                                log.error("Not supported value type: {}" , val.getClass().getName());
//                            }
//                        }
//                    }
//                });
//        //noinspection unchecked
//        return (T)builder.build();
//    }
//
//    private static <T, R> TypeToken<R> typeParam(TypeToken<T> token) {
//        //noinspection unchecked
//        return Optional
//                .of(token.type())
//                .flatMap(Optionals.ofType(ParameterizedType.class))
//                .map(ParameterizedType::getActualTypeArguments)
//                .filter(t -> t.length == 1)
//                .map(t -> t[0])
//                .flatMap(Optionals.ofType(Class.class))
//                .map(c -> (Class<R>)c)
//                .map(TypeToken::of)
//                .orElse(null);
//    }
//
//    private static Observable<OResult> toObservable(OResultSet resultSet) {
//        return Observable.create(emitter -> {
//            emitter.setCancellable(resultSet::close);
//            resultSet.stream().forEach(emitter::onNext);
//            emitter.onComplete();
//        });
//    }
//}
