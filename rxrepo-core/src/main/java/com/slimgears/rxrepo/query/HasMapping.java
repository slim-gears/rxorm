package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;

public interface HasMapping<S, T> {
    @Nullable
    ObjectExpression<S, T> mapping();
}
