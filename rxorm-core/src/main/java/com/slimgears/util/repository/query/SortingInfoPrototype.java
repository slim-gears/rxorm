package com.slimgears.util.repository.query;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.repository.expressions.PropertyExpression;

@AutoValuePrototype
public interface SortingInfoPrototype<S, T, V> {
    PropertyExpression<S, T, V> property();
    boolean ascending();
}
