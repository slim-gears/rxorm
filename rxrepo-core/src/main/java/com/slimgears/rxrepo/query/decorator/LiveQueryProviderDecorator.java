package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.rxrepo.util.PropertyExpressions;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
                        .properties(findRequiredProperties(query))
                        .build())
                .compose(Notifications.applyQuery(query));
    }

    private <K, S, T> ImmutableSet<PropertyExpression<S, ?, ?>> findRequiredProperties(QueryInfo<K, S, T> query) {
        return ImmutableSet.<PropertyExpression<S, ?, ?>>builder()
                .addAll(unmapProperties(query.properties(), query.mapping()))
                .addAll(PropertyExpressions.allReferencedProperties(query.mapping()))
                .addAll(PropertyExpressions.allReferencedProperties(query.predicate()))
                .build();
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(
                unmapQuery(queryInfo),
                QueryInfo.<K, S, S>builder()
                        .metaClass(observeInfo.metaClass())
                        .build())
                .compose(Notifications.applyQuery(queryInfo));
    }

    @SuppressWarnings("unchecked")
    private static <K, S, T> QueryInfo<K, S, S> unmapQuery(QueryInfo<K, S, T> query) {
        QueryInfo<K, S, S> unmappedQuery = Optional.ofNullable(query.mapping())
                .filter(m -> m.type().operationType() != Expression.OperationType.Argument)
                .map(m -> QueryInfo
                        .<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .sorting(query.sorting())
                        .predicate(query.predicate())
                        .limit(query.limit())
                        .skip(query.skip())
                        .properties(unmapProperties(query.properties(), query.mapping()))
                        .build())
                .orElseGet(() -> (QueryInfo<K, S, S>)query);
        log.trace("Unmapped query: {} -> {}", query, unmappedQuery);
        return unmappedQuery;
    }

    private static <S, T> ImmutableSet<PropertyExpression<S, ?, ?>> unmapProperties(ImmutableSet<PropertyExpression<T, ?, ?>> properties, ObjectExpression<S, T> mapping) {
        return Optional.ofNullable(properties)
                .<ImmutableSet<PropertyExpression<S, ?, ?>>>map(pp -> pp.stream().map(p -> unmapProperty(p, mapping)).collect(ImmutableSet.toImmutableSet()))
                .orElse(null);
    }

    private static <S, T> PropertyExpression<S, ?, ?> unmapProperty(PropertyExpression<T, ?, ?> propertyExpression, ObjectExpression<S, T> mapping) {
        PropertyExpression<S, ?, ?> unmappedProp = Expressions.compose(mapping, propertyExpression);
        log.trace("Unmapped property (mapping: {}) {} -> {}", mapping, propertyExpression, unmappedProp);
        return unmappedProp;
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query)
            .throttleLatest(2000, TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }
}
