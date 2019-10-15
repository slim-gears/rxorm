package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.StringExpression;

@AutoValue
public abstract class StringConstantExpression<S>
    extends AbstractConstantExpression<S, String>
    implements StringExpression<S> {
    @JsonCreator
    public static <S> StringConstantExpression<S> create(@JsonProperty("type") Type type, @JsonProperty String value) {
        return new AutoValue_StringConstantExpression<>(type, value);
    }
}
