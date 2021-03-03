package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.LockProvider;
import com.slimgears.rxrepo.util.LockProviders;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.List;
import java.util.function.Supplier;

public class LockQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final LockProvider lockProvider;

    public static QueryProvider.Decorator create(LockProvider lockProvider) {
        return src -> new LockQueryProviderDecorator(src, lockProvider);
    }

    private LockQueryProviderDecorator(QueryProvider underlyingProvider, LockProvider lockProvider) {
        super(underlyingProvider);
        this.lockProvider = lockProvider;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return super.insert(metaClass, entities)
                .compose(LockProviders.forCompletable(lockProvider));
    }

    @Override
    public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return super.insertOrUpdate(metaClass, entities)
                .compose(LockProviders.forCompletable(lockProvider));
    }

    @Override
    public <K, S> Maybe<Single<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, entityUpdater)
                .compose(LockProviders.forMaybe(lockProvider));
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return super.update(update)
                .compose(LockProviders.forSingle(lockProvider));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return super.delete(delete)
                .compose(LockProviders.forSingle(lockProvider));
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return super.drop(metaClass)
                .compose(LockProviders.forCompletable(lockProvider));
    }

    @Override
    public Completable dropAll() {
        return super.dropAll()
                .compose(LockProviders.forCompletable(lockProvider));
    }

    @Override
    public void close() {
        lockProvider.withLock(super::close);
    }
}
