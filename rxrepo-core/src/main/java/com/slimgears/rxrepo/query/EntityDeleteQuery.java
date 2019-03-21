package com.slimgears.rxrepo.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Completable;

public interface EntityDeleteQuery<K, S extends HasMetaClassWithKey<K, S>>
        extends QueryBuilder<EntityDeleteQuery<K, S>, K, S> {
    Completable execute();
}
