package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.StringExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

@AutoValue
public abstract class StringPropertyExpression<S, T>
    extends AbstractPropertyExpression<S, T, String>
    implements StringExpression<S> {
    @JsonCreator
    public static <S, T> StringPropertyExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, String> property) {
        return new AutoValue_StringPropertyExpression<>(type, target, property);
    }

    @Override
    protected ObjectExpression<S, String> createConverted(ObjectExpression<S, T> newTarget) {
        return create(type(), newTarget, property());
    }
}
