package com.slimgears.rxrepo.mem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.encoding.MetaObjectResolver;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Streams;
import io.reactivex.Observable;
import io.reactivex.*;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MemoryEntityQueryProvider<K, S> implements EntityQueryProvider<K, S> {
    private final static Logger log = LoggerFactory.getLogger(MemoryEntityQueryProvider.class);
    private final AtomicLong sequenceNumber;
    private final MetaClassWithKey<K, S> metaClass;
    private final MetaObjectResolver objectResolver;
    private final Map<K, ObjectReference<S>> objects = new ConcurrentHashMap<>();
    private final Subject<Notification<S>> notificationSubject = PublishSubject.create();
    private final Lazy<List<PropertyMeta<S, ?>>> referenceProperties;

    private static class ObjectReference<S> {
        private final AtomicReference<S> reference = new AtomicReference<>();
        private final AtomicLong modificationSequenceNum;
        private final AtomicLong sequenceNum;

        private ObjectReference(AtomicLong sequenceNum) {
            this.sequenceNum = sequenceNum;
            this.modificationSequenceNum = new AtomicLong(sequenceNum.incrementAndGet());
        }

        public boolean compareAndSet(S expectedObj, S newObj) {
            if (reference.compareAndSet(expectedObj, newObj)) {
                modificationSequenceNum.set(sequenceNum.incrementAndGet());
                return true;
            }
            return false;
        }

        public S get() {
            return reference.get();
        }
    }

    private MemoryEntityQueryProvider(MetaClassWithKey<K, S> metaClass,
                                      MetaObjectResolver objectResolver,
                                      AtomicLong sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        this.metaClass = metaClass;
        this.objectResolver = objectResolver;
        this.referenceProperties = Lazy.of(() -> Streams
                .fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .collect(ImmutableList.toImmutableList()));
    }

    static <K, S> MemoryEntityQueryProvider<K, S> create(
            MetaClassWithKey<K, S> metaClass,
            MetaObjectResolver objectResolver,
            AtomicLong sequenceNumber) {
        return new MemoryEntityQueryProvider<>(metaClass, objectResolver, sequenceNumber);
    }

    @Override
    public MetaClassWithKey<K, S> metaClass() {
        return metaClass;
    }

    @Override
    public Maybe<Supplier<S>> insertOrUpdate(K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return Maybe.defer(() -> {
            sequenceNumber.incrementAndGet();
            Supplier<ObjectReference<S>> referenceResolver = () -> objects.computeIfAbsent(key, k -> new ObjectReference<>(sequenceNumber));
            S oldValue = referenceResolver.get().get();
                        return entityUpdater
                    .apply(Optional.ofNullable(referenceResolver.get().get()).map(Maybe::just).orElseGet(Maybe::empty))
                    .flatMap(e -> referenceResolver.get().compareAndSet(oldValue, e)
                            ? (e != null ? Maybe.just(e): Maybe.empty())
                                        : Maybe.error(new ConcurrentModificationException("Concurrent modification of " + metaClass.simpleName() + " detected")))
                                .doOnSuccess(e -> {
                                    if (!Objects.equals(oldValue, e)) {
                            Notification<S> notification = Notification.ofModified(oldValue, e, sequenceNumber.get());
                                        notificationSubject.onNext(notification);
                                        log.debug("Published notification: {}", notification);
                                    }
                                })
                                .map(e -> () -> e);
                    });
    }

    @Override
    public <T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        log.trace("Querying {}", query);
        Predicate<S> predicate = Expressions.compileRxPredicate(query.predicate());
        java.util.function.Function<S, T> mapper = Expressions.compile(query.mapping());
        return Observable.fromIterable(objects.values())
                .flatMapMaybe(ref -> Maybe.fromCallable(ref::get)
                        .doOnSuccess(ob -> Expressions.sequenceNumber().set(ref.modificationSequenceNum.get()))
                        .filter(predicate)
                        .map(o -> Notification.ofCreated(o, ref.modificationSequenceNum.get())))
                .compose(ob -> Optional.ofNullable(query.sorting()).map(this::toNotificationComparator).map(ob::sorted).orElse(ob))
                .compose(ob -> Optional.ofNullable(query.skip()).map(ob::skip).orElse(ob))
                .compose(ob -> Optional.ofNullable(query.limit()).map(ob::take).orElse(ob))
                .doOnNext(val -> log.trace("Object without references: {}", val))
                .flatMapSingle(this::applyReferences)
                .doOnNext(val -> log.trace("Object with references: {}", val))
                .map(n -> n.map(mapper))
                .doOnNext(val -> log.trace("Object after mapping: {}", val))
                .compose(ob -> Optional
                        .ofNullable(query.distinct())
                        .filter(Boolean::booleanValue)
                        .map(b -> ob.distinct(Notification::newValue))
                        .orElse(ob))
                .doOnNext(val -> log.trace("Object after distinct: {}", val))
                .compose(ob -> Optional
                        .ofNullable(query.properties())
                        .filter(p -> !p.isEmpty())
                        .map(p -> ob.map(n -> n.map(maskProperties(p))))
                        .orElse(ob))
                .doOnNext(val -> log.trace("Object after masking properties: {}", val))
                .doOnNext(val -> log.trace("Emitting object: {}", val));
    }

    private <T> Comparator<Notification<T>> toNotificationComparator(Iterable<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos) {
        return Optional.ofNullable(SortingInfos.toComparator(sortingInfos))
                .map(c -> Comparator.<Notification<T>, T>comparing(Notification::newValue, c))
                .orElse(null);
    }

    private <T> java.util.function.Function<T, T> maskProperties(ImmutableSet<PropertyExpression<T, ?, ?>> properties) {
        if (properties.isEmpty()) {
            return java.util.function.Function.identity();
        }

        Set<String> paths = Streams
                .fromIterable(properties)
                .map(PropertyExpressions::pathOf)
                .collect(Collectors.toSet());

        return obj -> Optional
                .ofNullable(obj)
                .map(o -> maskProperties(o, paths, ""))
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static <T> T maskProperties(T obj, Set<String> propertiesToRetain, String currentPrefix) {
        MetaBuilder<T> builder = ((HasMetaClass<T>)obj).toBuilder();
        MetaClass<T> metaClass = ((HasMetaClass<T>)obj).metaClass();
        List<PropertyExpression<T, T, ?>> ownProperties = PropertyExpressions.ownPropertiesOf(metaClass.asType())
                .collect(Collectors.toList());

        ownProperties.stream()
                .filter(p -> !propertiesToRetain.contains(currentPrefix + PropertyExpressions.pathOf(p)))
                .map(PropertyExpression::property)
                .filter(p -> !PropertyMetas.isMandatory(p))
                .filter(p -> !PropertyMetas.hasMetaClass(p))
                .forEach(p -> p.setValue(builder, null));

        ownProperties
                .stream()
                .map(PropertyExpression::property)
                .filter(PropertyMetas::hasMetaClass)
                .filter(p -> p.getValue(obj) != null)
                .forEach(p -> replaceValue(obj, builder, p, v -> maskProperties(v, propertiesToRetain, currentPrefix + p.name() + ".")));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <T> void replaceValue(T obj, MetaBuilder<T> builder, PropertyMeta<T, ?> prop, java.util.function.Function<Object, Object> updater) {
        Object currentVal = prop.getValue(obj);
        Object newVal = updater.apply(currentVal);
        if (currentVal != newVal) {
            ((PropertyMeta<T, Object>)prop).setValue(builder, newVal);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return notificationSubject
                .doOnNext(n -> Expressions.sequenceNumber().set(n.sequenceNumber()))
                .compose(src -> Optional.ofNullable(query.mapping())
                        .map(Expressions::compile)
                        .map(m -> src.map(nn -> nn.map(m)))
                        .orElse((Observable<Notification<T>>)(Observable)src))
                .doOnNext(n -> log.debug("Notification --> {}", n));
    }

    @Override
    public <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return query(query)
                .map(Notification::newValue)
                .toList()
                .toMaybe()
                .flatMap(list -> {
                    CollectionExpression<T, T, Collection<T>> collection = ConstantExpression.of(list);
                    UnaryOperationExpression<T, Collection<T>, R> aggregated = aggregator.apply(collection);
                    java.util.function.Function<T, R> aggFunc = Expressions.compile(aggregated);
                    return Optional
                            .ofNullable(aggFunc.apply(null))
                            .map(Maybe::just)
                            .orElseGet(Maybe::empty);
                });
    }

    @Override
    public Single<Integer> update(UpdateInfo<K, S> update) {
        return Single.error(() -> new UnsupportedOperationException("Not supported yet"));
    }

    @Override
    public Single<Integer> delete(DeleteInfo<K, S> delete) {
        Predicate<S> predicate = Expressions.compileRxPredicate(delete.predicate());
        return Observable
                .fromIterable(objects.values())
                .doOnSubscribe(d -> sequenceNumber.incrementAndGet())
                .map(ObjectReference::get)
                .filter(predicate)
                .compose(ob -> Optional.ofNullable(delete.limit()).map(ob::take).orElse(ob))
                .map(metaClass::keyOf)
                .filter(key -> Optional
                        .ofNullable(objects.remove(key))
                        .map(ref -> Notification.ofDeleted(ref.get(), ref.modificationSequenceNum.get()))
                        .map(n -> {
                            notificationSubject.onNext(n);
                            return true;
                        })
                        .orElse(false)
                )
                .count()
                .map(Long::intValue);
    }

    @Override
    public Completable drop() {
        return Completable.fromAction(objects::clear);
    }

    @SuppressWarnings("unchecked")
    private Single<Notification<S>> applyReferences(Notification<S> entity) {
        if (referenceProperties.get().isEmpty()) {
            return Single.just(entity);
        }
        MetaBuilder<S> builder = Objects.requireNonNull((HasMetaClass<S>)entity.newValue()).toBuilder();
        return Observable.fromIterable(referenceProperties.get())
                .flatMapMaybe(p -> retrieveReference(entity.newValue(), p))
                .doOnNext(c -> c.accept(builder))
                .ignoreElements()
                .andThen(Single.fromCallable(builder::build))
                .map(val -> Notification.ofCreated(val, entity.sequenceNumber()));
    }

    private <V> Maybe<Consumer<MetaBuilder<S>>> retrieveReference(S entity, PropertyMeta<S, V> propertyMeta) {
        MetaClassWithKey<?, V> meta = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
        V ref = propertyMeta.getValue(entity);
        return resolve(meta, ref)
                .map(newRef -> builder -> propertyMeta.setValue(builder, newRef));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <_K, _S> Maybe<_S> resolve(MetaClassWithKey<_K, _S> meta, _S value) {
        if (value == null) {
            return Maybe.empty();
        }
        _K key = meta.keyOf(value);
        return objectResolver.resolve((MetaClassWithKey)meta, key);
    }

    Maybe<S> find(K key) {
        return Maybe.fromCallable(() -> objects.get(key)).map(ObjectReference::get);
    }

    @Override
    public void close() {

    }
}
