package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.reflect.TypeToken;

public interface ArgumentExpression<S, T> extends ObjectExpression<S, T> {
    @JsonProperty TypeToken<T> argType();
}
