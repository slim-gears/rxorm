package com.slimgears.rxrepo.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Single;

public interface EntityDeleteQuery<K, S extends HasMetaClassWithKey<K, S>>
        extends QueryBuilder<EntityDeleteQuery<K, S>, K, S> {
    Single<Integer> execute();
}
