package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.Expressions;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
                        .build())
                .compose(Notifications.applyQuery(query));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo) {
        return queryAndObserve(
                queryInfo,
                queryInfo.toBuilder()
                        .limit(null)
                        .skip(null)
                        .sorting(ImmutableList.of())
                        .build());
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
        return Optional.ofNullable(query.mapping())
                .map(q -> QueryInfo
                        .<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .sorting(query.sorting())
                        .predicate(query.predicate())
                        .limit(query.limit())
                        .skip(query.skip())
                        .properties(unmapProperties(query.properties(), query.mapping()))
                        .build())
                .orElseGet(() -> (QueryInfo<K, S, S>)query);
    }

    private static <S, T> ImmutableList<PropertyExpression<S, ?, ?>> unmapProperties(ImmutableList<PropertyExpression<T, ?, ?>> properties, ObjectExpression<S, T> mapping) {
        return properties.stream().map(p -> Expressions.compose(mapping, p)).collect(ImmutableList.toImmutableList());
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query)
            .debounce(500, TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }
}
