package com.slimgears.rxrepo.mem;

import com.google.common.collect.ImmutableList;
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
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MemoryEntityQueryProvider<K, S extends HasMetaClassWithKey<K, S>> implements EntityQueryProvider<K, S> {
    private final static Logger log = LoggerFactory.getLogger(MemoryEntityQueryProvider.class);
    private final MetaClassWithKey<K, S> metaClass;
    private final MetaObjectResolver objectResolver;
    private final Map<K, AtomicReference<S>> objects = new ConcurrentHashMap<>();
    private final Subject<Notification<S>> notificationSubject = PublishSubject.create();
    private final Lazy<List<PropertyMeta<S, ?>>> referenceProperties;
    private final Scheduler notificationScheduler;
    private final Scheduler updateScheduler;

    private MemoryEntityQueryProvider(MetaClassWithKey<K, S> metaClass,
                                      MetaObjectResolver objectResolver,
                                      Scheduler notificationScheduler,
                                      Scheduler updateScheduler) {
        this.metaClass = metaClass;
        this.objectResolver = objectResolver;
        this.referenceProperties = Lazy.of(() -> Streams
                .fromIterable(metaClass.properties())
                .filter(PropertyMetas::isReference)
                .collect(ImmutableList.toImmutableList()));
        this.notificationScheduler = notificationScheduler;
        this.updateScheduler = updateScheduler;
    }

    static <K, S extends HasMetaClassWithKey<K, S>> EntityQueryProvider<K, S> create(
            MetaClassWithKey<K, S> metaClass,
            MetaObjectResolver objectResolver,
            Scheduler notificationScheduler,
            Scheduler updateScheduler) {
        return new MemoryEntityQueryProvider<>(metaClass, objectResolver, notificationScheduler, updateScheduler);
    }

    @Override
    public Maybe<S> insertOrUpdate(K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return Maybe.defer(() -> {
            AtomicReference<S> reference = objects.computeIfAbsent(key, k -> new AtomicReference<>());
            AtomicReference<S> oldValue = new AtomicReference<>(reference.get());
            return entityUpdater
                    .apply(Optional.ofNullable(reference.get()).map(Maybe::just).orElseGet(Maybe::empty))
                    .flatMap(e -> reference.compareAndSet(oldValue.get(), e)
                            ? Maybe.just(e)
                            : Maybe.error(new ConcurrentModificationException("Concurrent modification of " + metaClass.simpleName() + " detected")))
                    .doOnSuccess(e -> {
                        if (!Objects.equals(oldValue.get(), e)) {
                            Notification<S> notification = Notification.ofModified(oldValue.get(), e);
                            notificationSubject.onNext(notification);
                            log.debug("Published notification: {}", notification);
                        }
                    })
                    .subscribeOn(updateScheduler);
        });
    }

    @Override
    public <T> Observable<T> query(QueryInfo<K, S, T> query) {
        Predicate<S> predicate = Expressions.compileRxPredicate(query.predicate());
        Function<S, T> mapper = Expressions.compileRx(query.mapping());
        return Observable.fromIterable(objects.values())
                .flatMapMaybe(val -> Maybe.fromCallable(val::get))
                .filter(predicate)
                .compose(ob -> Optional.ofNullable(query.sorting()).map(SortingInfos::toComparator).map(ob::sorted).orElse(ob))
                .compose(ob -> Optional.ofNullable(query.skip()).map(ob::skip).orElse(ob))
                .compose(ob -> Optional.ofNullable(query.limit()).map(ob::take).orElse(ob))
                .flatMapSingle(this::applyReferences)
                .map(mapper)
                .compose(ob -> Optional
                        .ofNullable(query.distinct())
                        .filter(Boolean::booleanValue)
                        .map(b -> ob.distinct())
                        .orElse(ob))
                .compose(ob -> Optional
                        .ofNullable(query.properties())
                        .filter(p -> !p.isEmpty())
                        .map(p -> ob.map(maskProperties(p)::apply))
                        .orElse(ob));
    }

    private <T> java.util.function.Function<T, T> maskProperties(ImmutableList<PropertyExpression<T, ?, ?>> properties) {
        if (properties.isEmpty()) {
            return java.util.function.Function.identity();
        }

        Set<String> paths = Streams
                .fromIterable(properties)
                .map(PropertyExpressions::toPath)
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
                .filter(p -> !propertiesToRetain.contains(currentPrefix + PropertyExpressions.toPath(p)))
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return notificationSubject
                .doOnSubscribe(d -> log.debug("Subscribed!!!"))
                .doOnNext(n -> log.debug("Notification: {}", n))
                .observeOn(notificationScheduler)
                .compose(src -> Optional.ofNullable(query.mapping())
                        .map(Expressions::compile)
                        .map(m -> src.map(nn -> nn.map(m)))
                        .orElse((Observable<Notification<T>>)(Observable)src));
    }

    @Override
    public <T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return query(query).toList()
                .toMaybe()
                .map(list -> {
                    CollectionExpression<T, T, Collection<T>> collection = ConstantExpression.of(list);
                    UnaryOperationExpression<T, Collection<T>, R> aggregated = aggregator.apply(collection);
                    java.util.function.Function<T, R> aggFunc = Expressions.compile(aggregated);
                    return aggFunc.apply(null);
                });
    }

    @Override
    public Single<Integer> update(UpdateInfo<K, S> update) {
        return Single.just(0);
    }

    @Override
    public Single<Integer> delete(DeleteInfo<K, S> delete) {
        Predicate<S> predicate = Expressions.compileRxPredicate(delete.predicate());
        return Observable
                .fromIterable(objects.values())
                .map(AtomicReference::get)
                .filter(predicate)
                .compose(ob -> Optional.ofNullable(delete.limit()).map(ob::take).orElse(ob))
                .map(HasMetaClassWithKey::keyOf)
                .filter(key -> Optional
                        .ofNullable(objects.remove(key))
                        .map(AtomicReference::get)
                        .map(e -> {
                            notificationSubject.onNext(Notification.ofDeleted(e));
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

    private Single<S> applyReferences(S entity) {
        if (referenceProperties.get().isEmpty()) {
            return Single.just(entity);
        }
        MetaBuilder<S> builder = entity.toBuilder();
        return Observable.fromIterable(referenceProperties.get())
                .flatMapMaybe(p -> retrieveReference(entity, p))
                .doOnNext(c -> c.accept(builder))
                .ignoreElements()
                .andThen(Single.fromCallable(builder::build));
    }

    private <V> Maybe<Consumer<MetaBuilder<S>>> retrieveReference(S entity, PropertyMeta<S, V> propertyMeta) {
        MetaClassWithKey<?, V> meta = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
        V ref = propertyMeta.getValue(entity);
        return resolve(meta, ref)
                .map(newRef -> builder -> propertyMeta.setValue(builder, newRef));
    }

    @SuppressWarnings("unchecked")
    private <_K, _S> Maybe<_S> resolve(MetaClassWithKey<_K, _S> meta, _S value) {
        if (value == null) {
            return Maybe.empty();
        }
        _K key = meta.keyOf(value);
        return objectResolver.resolve((MetaClassWithKey)meta, key);
    }

    Maybe<S> find(K key) {
        return Maybe.fromCallable(() -> objects.get(key)).map(AtomicReference::get);
    }
}
