package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryInfos;
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
                        .properties(QueryInfos.allReferencedProperties(query))
                        .build())
                .compose(Notifications.applyQuery(query));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(
                QueryInfos.unmapQuery(queryInfo),
                QueryInfo.<K, S, S>builder()
                        .metaClass(observeInfo.metaClass())
                        .build())
                .compose(Notifications.applyQuery(queryInfo));
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query)
            .throttleLatest(2000, TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }
}
