package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.query.Notification;
import io.reactivex.Observable;

public class QueryListeners {
    private static final QueryPublisher.OnQueryListener emptyOnQuery = new QueryPublisher.OnQueryListener() {
        @Override
        public <K, S, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult) {
            return queryResult;
        }
    };

    private static final QueryPublisher.OnLiveQueryListener emptyOnLiveQuery = new QueryPublisher.OnLiveQueryListener() {
        @Override
        public <K, S, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications) {
            return notifications;
        }
    };

    private static final QueryPublisher.QueryListener emptyQueryListener = create(emptyOnQuery, emptyOnLiveQuery);

    public static QueryPublisher.QueryListener create(QueryPublisher.OnQueryListener onQueryListener, QueryPublisher.OnLiveQueryListener onLiveQueryListener) {
        return new QueryPublisher.QueryListener() {
            @Override
            public <K, S, T> Observable<Notification<T>> onLiveQuery(QueryInfo<K, S, T> queryInfo, Observable<Notification<T>> notifications) {
                return onLiveQueryListener.onLiveQuery(queryInfo, notifications);
            }

            @Override
            public <K, S, T> Observable<T> onQuery(QueryInfo<K, S, T> queryInfo, Observable<T> queryResult) {
                return onQueryListener.onQuery(queryInfo, queryResult);
            }
        };
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryPublisher.QueryListener fromOnQuery(QueryPublisher.OnQueryListener onQueryListener) {
        return create(onQueryListener, emptyOnLiveQuery);
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryPublisher.QueryListener fromOnLiveQuery(QueryPublisher.OnLiveQueryListener onLiveQuery) {
        return create(emptyOnQuery, onLiveQuery);
    }
}
