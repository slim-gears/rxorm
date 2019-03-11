package com.slimgears.util.repository.query;

import com.slimgears.util.repository.expressions.ObjectExpression;

import javax.annotation.Nullable;

public interface HasMapping<S, T> {
    @Nullable ObjectExpression<S, T> mapping();
}
