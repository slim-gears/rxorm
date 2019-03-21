package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;

@AutoValuePrototype
public interface SortingInfoPrototype<S, T, V> {
    PropertyExpression<S, T, V> property();
    boolean ascending();
}
