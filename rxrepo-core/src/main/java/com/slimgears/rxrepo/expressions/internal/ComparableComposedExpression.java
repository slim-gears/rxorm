package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ComparableExpression;
import com.slimgears.rxrepo.expressions.ComposedExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@AutoValue
public abstract class ComparableComposedExpression<S, T, R extends Comparable<R>>
    extends AbstractComposedExpression<S, T, R>
    implements ComparableExpression<S, R> {
    @JsonCreator
    public static <S, T, R extends Comparable<R>> ComparableComposedExpression<S, T, R> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, T> source,
            @JsonProperty("expression") ObjectExpression<T, R> expression) {
        return new AutoValue_ComparableComposedExpression<>(type, source, expression);
    }

    @Override
    protected ObjectExpression<S, R> createConverted(ObjectExpression<S, T> newSource, ObjectExpression<T, R> newExpression) {
        return create(type(), newSource, newExpression);
    }
}
