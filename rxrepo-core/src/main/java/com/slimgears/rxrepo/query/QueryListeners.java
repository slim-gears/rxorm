package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;

public class QueryListeners {
    private static final Repository.OnQueryListener emptyOnQuery = new Repository.OnQueryListener() {
        @Override
        public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult) {
            return queryResult;
        }
    };

    private static final Repository.OnLiveQueryListener emptyOnLiveQuery = new Repository.OnLiveQueryListener() {
        @Override
        public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications) {
            return notifications;
        }
    };

    private static final Repository.QueryListener emptyQueryListener = create(emptyOnQuery, emptyOnLiveQuery);

    public static Repository.QueryListener create(Repository.OnQueryListener onQueryListener, Repository.OnLiveQueryListener onLiveQueryListener) {
        return new Repository.QueryListener() {
            @Override
            public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications) {
                return onLiveQueryListener.onLiveQuery(queryInfo, notifications);
            }

            @Override
            public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult) {
                return onQueryListener.onQuery(queryInfo, queryResult);
            }
        };
    }

    @SuppressWarnings("WeakerAccess")
    public static Repository.QueryListener fromOnQuery(Repository.OnQueryListener onQueryListener) {
        return create(onQueryListener, emptyOnLiveQuery);
    }

    @SuppressWarnings("WeakerAccess")
    public static Repository.QueryListener fromOnLiveQuery(Repository.OnLiveQueryListener onLiveQuery) {
        return create(emptyOnQuery, onLiveQuery);
    }
}
