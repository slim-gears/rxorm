package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryInfos;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.util.autovalue.annotations.*;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LiveQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(LiveQueryProviderDecorator.class);

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

        QueryInfo<K, S, S> unmappedQuery = QueryInfos.unmapQuery(query);
        Map<PropertyExpression<S, S, ?>, ImmutableSet<PropertyExpression<S, ?, ?>>> propertiesByRoot = unmappedQuery.properties().stream()
                .collect(Collectors.groupingBy(PropertyExpressions::rootOf, ImmutableSet.toImmutableSet()));

        return propertiesByRoot.keySet()
                .stream()
                .filter(PropertyExpressions::isReference)
                .map(p -> observeReferenceProperty(unmappedQuery, p, propertiesByRoot.get(p)))
                .reduce(Observable::mergeWith)
                .<ObservableTransformer<Notification<S>, Notification<S>>>map(o -> src -> src.mergeWith(o))
                .orElse(src -> src);
    }

    private <K1, S1, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S1, ?, ?>> properties) {
        ImmutableSet<PropertyExpression<S2, ?, ?>> referenceProperties = properties.stream()
                .filter(p -> p != referenceProperty)
                .map(p -> PropertyExpressions.tailOf(p, referenceProperty))
                .collect(ImmutableSet.toImmutableSet());
        return observeReferenceProperty(query, referenceProperty, referenceProperties, MetaClasses.forTokenWithKeyUnchecked(referenceProperty.reflect().objectType()));
    }

    @SuppressWarnings("unchecked")
    private <K1, S1, K2, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S2, ?, ?>> properties, MetaClassWithKey<K2, S2> metaClassWithKey) {
        return observeReference(metaClassWithKey, properties)
                .filter(NotificationPrototype::isModify)
                .flatMap(n -> query(QueryInfo
                        .<K1, S1, S1>builder()
                        .metaClass(query.metaClass())
                        .properties(query.properties())
                        .predicate(combineWithReferenceId(query.predicate(), n.oldValue(), referenceProperty, metaClassWithKey))
                        .build())
                        .map(obj -> {
                            MetaBuilder<S1> builder = ((HasMetaClass<S1>)obj).toBuilder();
                            referenceProperty.property().setValue(builder, n.oldValue());
                            S1 oldValue = builder.build();
                            referenceProperty.property().setValue(builder, n.newValue());
                            S1 newValue = builder.build();
                            return Notification.create(oldValue, newValue, n.generation());
                        })
                );
    }

    private <S, KT, T> ObjectExpression<S, Boolean> combineWithReferenceId(ObjectExpression<S, Boolean> predicate, T referencedObject, PropertyExpression<S, S, T> referenceProperty, MetaClassWithKey<KT, T> metaClass) {
        ObjectExpression<S, Boolean> referencePredicate = PropertyExpression
                .ofObject(referenceProperty, metaClass.keyProperty())
                .eq(metaClass.keyOf(referencedObject));

        return Optional.ofNullable(predicate)
                .<ObjectExpression<S, Boolean>>map(p -> BooleanExpression.and(p, referencePredicate))
                .orElse(referencePredicate);
    }

    private <K, S> Observable<Notification<S>> observeReference(MetaClassWithKey<K, S> metaClass, ImmutableSet<PropertyExpression<S, ?, ?>> properties) {
        return liveQuery(QueryInfo.<K, S, S>builder()
                .metaClass(metaClass)
                .properties(properties)
                .build());
    }
}
