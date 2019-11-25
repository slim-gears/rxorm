package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.ComparableExpression;
import com.slimgears.util.reflect.TypeTokens;

@AutoValue
public abstract class ComparableArgumentExpression<S, T extends Comparable<T>>
    extends AbstractArgumentExpression<S, T>
    implements ComparableExpression<S, T> {
    @JsonCreator
    public static <S, T extends Comparable<T>> ComparableArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<T> argType) {
        return new AutoValue_ComparableArgumentExpression<>(type, TypeTokens.eliminateTypeVars(argType));
    }
}
