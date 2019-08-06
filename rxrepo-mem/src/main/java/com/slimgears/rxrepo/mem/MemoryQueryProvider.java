package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.encoding.MetaObjectResolver;
import com.slimgears.rxrepo.query.provider.AbstractEntityQueryProviderAdapter;
import com.slimgears.rxrepo.query.provider.EntityQueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MemoryQueryProvider extends AbstractEntityQueryProviderAdapter implements MetaObjectResolver {
    private final ExecutorService notificationExecutor = Executors.newSingleThreadExecutor();
    private final Scheduler notificationScheduler = Schedulers.from(notificationExecutor);
    private final ExecutorService updateExecutor = Executors.newFixedThreadPool(10);
    private final Scheduler updateScheduler = Schedulers.from(updateExecutor);

    @Override
    protected <K, S extends HasMetaClassWithKey<K, S>> EntityQueryProvider<K, S> createProvider(MetaClassWithKey<K, S> metaClass) {
        return MemoryEntityQueryProvider.create(metaClass,
                this,
                notificationScheduler,
                updateScheduler);
    }

    @Override
    protected Scheduler scheduler() {
        return Schedulers.computation();
    }

    @Override
    protected Completable dropAllProviders() {
        return Completable.complete();
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>> Maybe<S> resolve(MetaClassWithKey<K, S> metaClass, K key) {
        return ((MemoryEntityQueryProvider<K, S>)entities(metaClass)).find(key);
    }

    @Override
    public void close() {
        notificationExecutor.shutdown();
        updateExecutor.shutdown();
    }
}
