package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ComposedExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@AutoValue
public abstract class ObjectComposedExpression<S, T, R>
    extends AbstractComposedExpression<S, T, R>
    implements ObjectExpression<S, R> {
    @JsonCreator
    public static <S, T, R> ObjectComposedExpression<S, T, R> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, T> source,
            @JsonProperty("expression") ObjectExpression<T, R> expression) {
        return new AutoValue_ObjectComposedExpression<>(type, source, expression);
    }
}
