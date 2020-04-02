package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.encoding.MetaObjectResolver;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.AbstractEntityQueryProviderAdapter;
import com.slimgears.rxrepo.query.provider.EntityQueryProvider;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Safe;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryQueryProvider extends AbstractEntityQueryProviderAdapter implements MetaObjectResolver {
    private final List<AutoCloseable> closeableList = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong sequenceNumber = new AtomicLong();
    private final SchedulingProvider schedulingProvider;

    private MemoryQueryProvider(SchedulingProvider schedulingProvider) {
        this.schedulingProvider = schedulingProvider;
    }

    public static MemoryQueryProvider create(SchedulingProvider schedulingProvider) {
        return new MemoryQueryProvider(schedulingProvider);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(query)
                .observeOn(schedulingProvider.scheduler());
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(queryInfo, observeInfo)
                .observeOn(schedulingProvider.scheduler());
    }

    @Override
    protected <K, S> EntityQueryProvider<K, S> createProvider(MetaClassWithKey<K, S> metaClass) {
        MemoryEntityQueryProvider<K, S> provider = MemoryEntityQueryProvider.create(metaClass, this, sequenceNumber);
        closeableList.add(provider);
        return provider;
    }

    @Override
    protected Completable dropAllProviders() {
        return Completable.complete();
    }

    @Override
    public <K, S> Maybe<S> resolve(MetaClassWithKey<K, S> metaClass, K key) {
        return ((MemoryEntityQueryProvider<K, S>)entities(metaClass)).find(key);
    }

    @Override
    public void close() {
        closeableList.stream()
                .map(Safe::ofClosable)
                .forEach(Safe.Closeable::close);
    }
}
