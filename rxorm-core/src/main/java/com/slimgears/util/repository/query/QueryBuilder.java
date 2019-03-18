package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.repository.expressions.BooleanExpression;

public interface QueryBuilder<__B extends QueryBuilder<__B, K, S>, K, S extends HasMetaClassWithKey<K, S>> {
    __B where(BooleanExpression<S> predicate);
    __B limit(long limit);
    __B skip(long skip);
}
