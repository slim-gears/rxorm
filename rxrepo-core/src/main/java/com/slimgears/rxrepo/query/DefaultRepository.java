package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class DefaultRepository implements Repository {
    private final QueryProvider queryProvider;
    private final Map<MetaClassWithKey<?, ?>, EntitySet<?, ?>> entitySetMap = new HashMap<>();
    private final List<QueryListener> listeners = new CopyOnWriteArrayList<>();

    public DefaultRepository(QueryProvider queryProvider) {
        this.queryProvider = queryProvider;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
        return (EntitySet<K, T>)entitySetMap.computeIfAbsent(meta, m -> createEntitySet(meta));
    }

    @Override
    public Disposable subscribe(QueryListener queryListener) {
        listeners.add(queryListener);
        return Disposables.fromAction(() -> listeners.remove(queryListener));
    }

    private <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> createEntitySet(MetaClassWithKey<K, T> metaClass) {
        return DefaultEntitySet.create(queryProvider, metaClass, listeners);
    }
}
