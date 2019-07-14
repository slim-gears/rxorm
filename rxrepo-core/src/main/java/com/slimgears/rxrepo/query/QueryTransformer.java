package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.ObservableTransformer;

import java.util.List;

public interface QueryTransformer<K, S extends HasMetaClassWithKey<K, S>, T, R> {
    ObservableTransformer<List<Notification<T>>, R> transformer(QueryInfo<K, S, T> query);

    static <K, S extends HasMetaClassWithKey<K, S>, T, R> QueryTransformer<K, S, T, R> of(
            ObservableTransformer<List<Notification<T>>, R> transformer) {
        return query -> transformer;
    }
}
