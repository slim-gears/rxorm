package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.repository.expressions.PropertyExpression;

@AutoValuePrototype
public interface SortingInfoPrototype<S, T, B extends BuilderPrototype<T, B>, V> {
    PropertyExpression<S, T, B, V> property();
    boolean ascending();
}
