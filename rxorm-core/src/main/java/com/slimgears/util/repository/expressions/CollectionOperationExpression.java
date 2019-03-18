package com.slimgears.util.repository.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public interface CollectionOperationExpression<S, T, M, R> extends CollectionExpression<S, R> {
    @JsonProperty ObjectExpression<S, Collection<T>> source();
    @JsonProperty ObjectExpression<T, M> operation();
}
