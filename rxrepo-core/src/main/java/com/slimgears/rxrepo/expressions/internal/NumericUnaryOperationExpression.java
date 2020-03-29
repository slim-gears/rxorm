package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.NumericExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;

@AutoValue
public abstract class NumericUnaryOperationExpression<S, T, V extends Number & Comparable<V>>
    extends AbstractUnaryOperationExpression<S, T, V>
    implements NumericExpression<S, V> {
    @JsonCreator
    public static <S, T, V extends Number & Comparable<V>> NumericUnaryOperationExpression<S, T, V> create(
            @JsonProperty("type") Type type,
            @JsonProperty("operand") ObjectExpression<S, T> operand) {
        return new AutoValue_NumericUnaryOperationExpression<>(type, operand);
    }

    @Override
    protected ObjectExpression<S, V> createConverted(ObjectExpression<S, T> newOperand) {
        return create(type(), newOperand);
    }
}
