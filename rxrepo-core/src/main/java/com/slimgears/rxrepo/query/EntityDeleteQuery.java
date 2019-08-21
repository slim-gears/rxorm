package com.slimgears.rxrepo.query;

import io.reactivex.Single;

public interface EntityDeleteQuery<K, S>
        extends QueryBuilder<EntityDeleteQuery<K, S>, K, S> {
    Single<Integer> execute();
}
