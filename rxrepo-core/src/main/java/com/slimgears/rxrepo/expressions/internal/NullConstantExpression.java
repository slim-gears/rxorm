package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.util.reflect.TypeToken;

@AutoValue
public abstract class NullConstantExpression<S, V> implements ConstantExpression<S, V> {
    @JsonProperty public abstract TypeToken<V> nullType();

    @Override
    public TypeToken<V> objectType() {
        return nullType();
    }

    @JsonCreator
    public static <S, V> NullConstantExpression<S, V> create(
            @JsonProperty Type type,
            @JsonProperty TypeToken<V> nullType) {
        return new AutoValue_NullConstantExpression<>(type, null, nullType);
    }
}
