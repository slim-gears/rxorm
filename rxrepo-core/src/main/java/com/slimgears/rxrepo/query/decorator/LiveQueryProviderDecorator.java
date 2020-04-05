package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.expressions.internal.NumericUnaryOperationExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryInfos;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaBuilder;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LiveQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private LiveQueryProviderDecorator(QueryProvider upstream) {
        super(upstream);
    }

    public static QueryProvider.Decorator create() {
        return LiveQueryProviderDecorator::new;
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(QueryInfo.<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .properties(QueryInfos.allReferencedProperties(query))
                        .build())
                .compose(Notifications.applyFilter(query.predicate()))
                .compose(applyReferencedObserve(query))
                .compose(Notifications.applyMap(query.mapping()))
                .compose(Notifications.applyFieldsFilter(query.properties()));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(
                QueryInfos.unmapQuery(queryInfo),
                QueryInfo.<K, S, S>builder()
                        .properties(QueryInfos.allReferencedProperties(queryInfo))
                        .metaClass(observeInfo.metaClass())
                        .build())
                .compose(Notifications.applyFilter(queryInfo.predicate()))
                .compose(applyReferencedObserve(queryInfo))
                .compose(Notifications.applyMap(queryInfo.mapping()))
                .compose(Notifications.applyFieldsFilter(queryInfo.properties()));
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query)
            .throttleLatest(2000, TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }

    private <K, S, T> ObservableTransformer<Notification<S>, Notification<S>> applyReferencedObserve(QueryInfo<K, S, T> query) {
        if (query.properties().isEmpty()) {
            return src -> src;
        }

        AtomicReference<Long> lastCreatedSequenceNumber = new AtomicReference<>();

        QueryInfo<K, S, S> unmappedQuery = QueryInfos.unmapQuery(query);
        Map<PropertyExpression<S, S, ?>, ImmutableSet<PropertyExpression<S, ?, ?>>> propertiesByRoot = unmappedQuery.properties().stream()
                .collect(Collectors.groupingBy(PropertyExpressions::rootOf, ImmutableSet.toImmutableSet()));

        ObservableTransformer<Notification<S>, Notification<S>> transformer = propertiesByRoot.keySet()
                .stream()
                .filter(PropertyExpressions::isReference)
                .map(p -> observeReferenceProperty(unmappedQuery, p, propertiesByRoot.get(p), lastCreatedSequenceNumber))
                .reduce(Observable::mergeWith)
                .<ObservableTransformer<Notification<S>, Notification<S>>>map(o -> src -> src.mergeWith(o))
                .orElse(src -> src);

        return src -> src
                .compose(transformer)
                .doOnNext(n -> Optional
                        .of(n)
                        .filter(Notification::isCreate)
                        .map(Notification::sequenceNumber)
                        .ifPresent(lastCreatedSequenceNumber::set));
    }

    private <K1, S1, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S1, ?, ?>> properties, AtomicReference<Long> lastCreatedSequenceNumber) {
        ImmutableSet<PropertyExpression<S2, ?, ?>> referenceProperties = properties.stream()
                .filter(p -> p != referenceProperty)
                .map(p -> PropertyExpressions.tailOf(p, referenceProperty))
                .collect(ImmutableSet.toImmutableSet());
        return observeReferenceProperty(query, referenceProperty, referenceProperties, MetaClasses.forTokenWithKeyUnchecked(referenceProperty.reflect().objectType()), lastCreatedSequenceNumber);
    }

    @SuppressWarnings("unchecked")
    private <K1, S1, K2, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S2, ?, ?>> properties, MetaClassWithKey<K2, S2> metaClassWithKey, AtomicReference<Long> lastCreatedSequenceNumber) {
        return observeReference(metaClassWithKey, properties)
                .filter(NotificationPrototype::isModify)
                .flatMapIterable(n -> query(QueryInfo
                        .<K1, S1, S1>builder()
                        .metaClass(query.metaClass())
                        .properties(PropertyExpressions.includeMandatoryProperties(query.objectType(), query.properties()))
                        .predicate(combineWithReferenceId(query.predicate(), n, referenceProperty, metaClassWithKey, lastCreatedSequenceNumber))
                        .build())
                        .map(Notification::newValue)
                        .map(obj -> {
                            MetaBuilder<S1> builder = ((HasMetaClass<S1>)obj).toBuilder();
                            referenceProperty.property().setValue(builder, n.oldValue());
                            S1 oldValue = builder.build();
                            referenceProperty.property().setValue(builder, n.newValue());
                            S1 newValue = builder.build();
                            return Notification.create(oldValue, newValue, n.sequenceNumber());
                        })
                        .blockingIterable());
    }

    private <S, KT, T> ObjectExpression<S, Boolean> combineWithReferenceId(ObjectExpression<S, Boolean> predicate, Notification<T> referencedObject, PropertyExpression<S, S, T> referenceProperty, MetaClassWithKey<KT, T> metaClass, AtomicReference<Long> lastCreatedSequenceNumber) {
        Long sequenceNum = Optional
                .ofNullable(lastCreatedSequenceNumber.get())
                .orElse(Optional.ofNullable(referencedObject.sequenceNumber()).orElse(null));

        BooleanExpression<S> referencePredicate = PropertyExpression
                .ofObject(referenceProperty, metaClass.keyProperty())
                .eq(metaClass.keyOf(referencedObject.oldValue()));

        BooleanExpression<S> seqNumPredicate = Optional.ofNullable(sequenceNum)
                .<BooleanExpression<S>>map(sn -> NumericUnaryOperationExpression.<S, T, Long>create(Expression.Type.SequenceNumber, ObjectExpression.objectArg(metaClass.asType()))
                        .lessThan(sn))
                .map(p -> BooleanExpression.and(referencePredicate, p))
                .orElse(referencePredicate);

        return Optional.ofNullable(predicate)
                .<ObjectExpression<S, Boolean>>map(p -> BooleanExpression.and(p, seqNumPredicate))
                .orElse(referencePredicate);
    }

    private <K, S> Observable<Notification<S>> observeReference(MetaClassWithKey<K, S> metaClass, ImmutableSet<PropertyExpression<S, ?, ?>> properties) {
        return liveQuery(QueryInfo.<K, S, S>builder()
                .metaClass(metaClass)
                .properties(properties)
                .build());
    }
}
