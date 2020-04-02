package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.LockProvider;
import com.slimgears.rxrepo.util.LockProviders;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;

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
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive)
                .compose(LockProviders.forCompletable(lockProvider));
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return super.insertOrUpdate(metaClass, entity, recursive)
                .compose(LockProviders.forSingle(lockProvider));
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, recursive, entityUpdater)
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
