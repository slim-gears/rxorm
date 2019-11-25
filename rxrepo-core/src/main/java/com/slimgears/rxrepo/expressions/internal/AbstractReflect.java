package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.ObjectExpression;

public abstract class AbstractReflect<S, T> implements ObjectExpression.Reflect<S, T> {
    private final ObjectExpression<S, T> expression;

    AbstractReflect(ObjectExpression<S, T> expression) {
        this.expression = expression;
    }

    @Override
    public ObjectExpression<S, T> expression() {
        return expression;
    }
}
