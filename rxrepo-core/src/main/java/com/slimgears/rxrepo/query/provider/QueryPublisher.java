package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;

public interface QueryPublisher {
    interface OnQueryListener {
        <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult);
    }

    interface OnLiveQueryListener {
        <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications);
    }

    interface QueryListener extends OnQueryListener, OnLiveQueryListener {
    }

    Disposable subscribe(QueryPublisher.QueryListener queryListener);

    default Disposable subscribe(QueryPublisher.OnQueryListener onQueryListener) {
        return subscribe(QueryListeners.fromOnQuery(onQueryListener));
    }

    default Disposable subscribe(QueryPublisher.OnLiveQueryListener onLiveQueryListener) {
        return subscribe(QueryListeners.fromOnLiveQuery(onLiveQueryListener));
    }
}
