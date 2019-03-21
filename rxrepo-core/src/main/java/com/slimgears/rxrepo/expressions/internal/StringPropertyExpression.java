package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.StringExpression;

@AutoValue
public abstract class StringPropertyExpression<S, T> implements PropertyExpression<S, T, String>, StringExpression<S> {
    @JsonCreator
    public static <S, T> StringPropertyExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, ? extends String> property) {
        return new AutoValue_StringPropertyExpression<>(type, target, property);
    }
}
