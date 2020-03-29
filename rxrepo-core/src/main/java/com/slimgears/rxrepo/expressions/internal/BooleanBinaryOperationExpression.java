package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.BinaryOperationExpression;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@AutoValue
public abstract class BooleanBinaryOperationExpression<S, V1, V2>
    extends AbstractBinaryOperationExpression<S, V1, V2, Boolean>
    implements BooleanExpression<S> {
    @JsonCreator
    public static <S, V1, V2> BooleanBinaryOperationExpression<S, V1, V2> create(
            @JsonProperty("type") Type type,
            @JsonProperty("left") ObjectExpression<S, V1> left,
            @JsonProperty("right") ObjectExpression<S, V2> right) {
        return new AutoValue_BooleanBinaryOperationExpression<>(type, left, right);
    }

    @Override
    protected ObjectExpression<S, Boolean> createConverted(ObjectExpression<S, V1> newLeft, ObjectExpression<S, V2> newRight) {
        return create(type(), newLeft, newRight);
    }
}
