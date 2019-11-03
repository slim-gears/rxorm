package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryInfo;
import io.reactivex.ObservableTransformer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public interface QueryTransformer<T, R> {
    <K, S> ObservableTransformer<List<Notification<S>>, R> transformer(QueryInfo<K, S, T> query, AtomicLong count);
}
