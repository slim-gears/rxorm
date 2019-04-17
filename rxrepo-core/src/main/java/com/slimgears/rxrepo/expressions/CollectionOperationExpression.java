package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public interface CollectionOperationExpression<S, T, M, R> extends CollectionExpression<S, R> {
    @JsonProperty ObjectExpression<S, ? extends Collection<T>> source();
    @JsonProperty ObjectExpression<T, M> operation();
}
