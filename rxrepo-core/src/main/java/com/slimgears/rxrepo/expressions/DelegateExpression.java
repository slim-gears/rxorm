package com.slimgears.rxrepo.expressions;

import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;

public interface DelegateExpression<S, T> extends ObjectExpression<S, T> {
    ObjectExpression<S, T> delegate();

    default BooleanExpression<S> searchText(String pattern) {
        return BooleanBinaryOperationExpression.create(Type.SearchText, this, ConstantExpression.of(pattern));
    }

    @Override
    default Reflect<S, T> reflect() {
        return delegate().reflect();
    }
}
