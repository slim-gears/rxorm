package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;

@PrototypeWithBuilder
public interface SortingInfoPrototype<S, T, V extends Comparable<V>> {
    PropertyExpression<S, T, V> property();
    boolean ascending();
}
