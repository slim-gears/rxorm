package com.slimgears.rxrepo.expressions;

public interface DelegateExpression<S, T> extends ObjectExpression<S, T> {
    ObjectExpression<S, T> delegate();
}
