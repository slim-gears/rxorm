package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

public interface QueryBuilder<__B extends QueryBuilder<__B, K, S>, K, S extends HasMetaClassWithKey<K, S>> {
    __B where(BooleanExpression<S> predicate);
    __B limit(long limit);
    __B skip(long skip);
}
