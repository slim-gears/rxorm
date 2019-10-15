package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

@AutoValue
public abstract class BooleanPropertyExpression<S, T>
    extends AbstractPropertyExpression<S, T, Boolean>
    implements BooleanExpression<S> {
    @JsonCreator
    public static <S, T> BooleanPropertyExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, Boolean> property) {
        return new AutoValue_BooleanPropertyExpression<>(type, target, property);
    }
}
