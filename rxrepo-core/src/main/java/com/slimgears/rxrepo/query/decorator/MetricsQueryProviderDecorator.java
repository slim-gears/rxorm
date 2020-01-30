package com.slimgears.rxrepo.query.decorator;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.*;
import io.reactivex.functions.Function;

import java.util.concurrent.atomic.AtomicReference;

public class MetricsQueryProviderDecorator implements QueryProvider.Decorator, MetricCollector.Binder, AutoCloseable {
    private final AtomicReference<MetricCollector> metricCollector;

    private MetricsQueryProviderDecorator(MetricCollector collector) {
        metricCollector = new AtomicReference<>(collector.name("rxrepo"));
    }

    public static MetricsQueryProviderDecorator create() {
        return new MetricsQueryProviderDecorator(MetricCollector.empty());
    }

    public static MetricsQueryProviderDecorator create(MetricCollector collector) {
        return new MetricsQueryProviderDecorator(collector);
    }

    @Override
    public void bindTo(MetricCollector.Factory factory) {
        metricCollector.set(factory.create().name("rxrepo"));
    }

    @Override
    public void close() {
        metricCollector.set(MetricCollector.empty());
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    class Decorator extends AbstractQueryProviderDecorator {
        protected Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
            return super
                    .insert(metaClass, entities)
                    .lift(asyncCollector("insert", metaClass).forCompletable());
        }

        @Override
        public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
            return super
                    .insertOrUpdate(metaClass, entity)
                    .lift(asyncCollector("insertOrUpdate", metaClass).forSingle());
        }

        @Override
        public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
            return super.insertOrUpdate(metaClass, key, entityUpdater)
                    .lift(asyncCollector("insertOrUpdateAtomic", metaClass).forMaybe());
        }

        @Override
        public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
            return super.query(query)
                    .lift(asyncCollector("query", query.metaClass()).forObservable());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query)
                    .lift(asyncCollector("liveQuery", query.metaClass()).forObservable());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> query) {
            return super.queryAndObserve(query)
                    .lift(asyncCollector("queryAndObserve", query.metaClass()).forObservable());
        }

        @Override
        public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.aggregate(query, aggregator)
                    .lift(asyncCollector("aggregate", query.metaClass()).forMaybe());
        }

        @Override
        public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.liveAggregate(query, aggregator)
                    .lift(asyncCollector("liveAggregate", query.metaClass()).forObservable());
        }

        @Override
        public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
            return super.update(update)
                    .lift(asyncCollector("batchUpdate", update.metaClass()).forSingle());
        }

        @Override
        public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
            return super.delete(delete)
                    .lift(asyncCollector("delete", delete.metaClass()).forSingle());
        }

        @Override
        public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
            return super.drop(metaClass)
                    .lift(asyncCollector("drop", metaClass).forCompletable());
        }

        private MetricCollector.Async asyncCollector(String operation, MetaClass<?> metaClass) {
            return metricCollector.get()
                    .name(metaClass.simpleName())
                    .name(operation)
                    .async()
                    .countSubscriptions("subscriptionCount")
                    .countCompletions("completeCount")
                    .countErrors("errorCount")
                    .countItems("itemCount")
                    .timeTillFirst("timeTillFirst")
                    .timeTillComplete("timeTillComplete")
                    .timeBetweenItems("timeBetweenItems");
        }
    }
}
