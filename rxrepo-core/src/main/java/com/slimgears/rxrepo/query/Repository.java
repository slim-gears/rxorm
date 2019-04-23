package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;

public interface Repository {
    <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta);
    Disposable subscribe(QueryListener queryListener);

    default Disposable subscribe(OnQueryListener onQueryListener) {
        return subscribe(QueryListeners.fromOnQuery(onQueryListener));
    }

    default Disposable subscribe(OnLiveQueryListener onLiveQueryListener) {
        return subscribe(QueryListeners.fromOnLiveQuery(onLiveQueryListener));
    }

    static Repository fromProvider(QueryProvider provider) {
        return new DefaultRepository(provider);
    }

    interface OnQueryListener {
        <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult);
    }

    interface OnLiveQueryListener {
        <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications);
    }

    interface QueryListener extends OnQueryListener, OnLiveQueryListener {
    }
}
