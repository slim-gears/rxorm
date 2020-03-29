package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.BinaryOperationExpression;
import com.slimgears.rxrepo.expressions.NumericExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@AutoValue
public abstract class NumericBinaryOperationExpression<S, T1, T2, V extends Number & Comparable<V>>
    extends AbstractBinaryOperationExpression<S, T1, T2, V>
    implements NumericExpression<S, V> {
    @JsonCreator
    public static <S, T1, T2, V extends Number & Comparable<V>> NumericBinaryOperationExpression<S, T1, T2, V> create(
            @JsonProperty("type") Type type,
            @JsonProperty("left") ObjectExpression<S, T1> left,
            @JsonProperty("right") ObjectExpression<S, T2> right) {
        return new AutoValue_NumericBinaryOperationExpression<>(type, left, right);
    }

    @Override
    protected ObjectExpression<S, V> createConverted(ObjectExpression<S, T1> newLeft, ObjectExpression<S, T2> newRight) {
        return create(type(), newLeft, newRight);
    }
}
