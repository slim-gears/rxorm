package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ComposedExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.StringExpression;

@AutoValue
public abstract class StringComposedExpression<S, T>
    extends AbstractComposedExpression<S, T, String>
    implements StringExpression<S> {
    @JsonCreator
    public static <S, T> StringComposedExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, T> source,
            @JsonProperty("expression") ObjectExpression<T, String> expression) {
        return new AutoValue_StringComposedExpression<>(type, source, expression);
    }

    @Override
    protected ObjectExpression<S, String> createConverted(ObjectExpression<S, T> newSource, ObjectExpression<T, String> newExpression) {
        return create(type(), newSource, newExpression);
    }
}
