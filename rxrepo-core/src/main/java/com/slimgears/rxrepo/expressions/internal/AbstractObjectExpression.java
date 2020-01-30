package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.stream.Lazy;

public abstract class AbstractObjectExpression<S, T> implements ObjectExpression<S, T> {
    private final Lazy<Reflect<S, T>> reflectLazy = Lazy.of(this::createReflect);

    @Override
    public Reflect<S, T> reflect() {
        return reflectLazy.get();
    }

    protected abstract Reflect<S, T> createReflect();

    @Override
    public String toString() {
        return type().name();
    }
}
