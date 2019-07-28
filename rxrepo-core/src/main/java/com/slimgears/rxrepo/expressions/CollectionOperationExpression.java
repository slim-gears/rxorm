package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public interface CollectionOperationExpression<S, T, M, R, CT extends Collection<T>, CR extends Collection<R>> extends CollectionExpression<S, R, CR> {
    @JsonProperty ObjectExpression<S, CT> source();
    @JsonProperty ObjectExpression<T, M> operation();
}
