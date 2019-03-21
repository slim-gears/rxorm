package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.util.reflect.TypeToken;

public interface ArgumentExpression<S, T> extends ObjectExpression<S, T> {
    @Override
    default TypeToken<? extends T> objectType() {
        return argType();
    }

    @JsonProperty TypeToken<T> argType();
}
