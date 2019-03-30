package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;

public interface HasPredicate<T> {
    @Nullable
    ObjectExpression<T, Boolean> predicate();
}
