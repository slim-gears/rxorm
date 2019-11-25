package com.slimgears.rxrepo.query;

import io.reactivex.Single;

public interface EntityDeleteQuery<S> extends QueryBuilder<EntityDeleteQuery<S>, S> {
    Single<Integer> execute();
}
