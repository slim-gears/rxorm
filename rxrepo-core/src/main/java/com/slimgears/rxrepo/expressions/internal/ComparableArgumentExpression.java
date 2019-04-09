package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.ComparableExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.reflect.TypeToken;

@AutoValue
public abstract class ComparableArgumentExpression<S, T extends Comparable<T>> implements ArgumentExpression<S, T>, ComparableExpression<S, T> {
    @JsonCreator
    public static <S, T extends Comparable<T>> ComparableArgumentExpression<S, T> create(
            @JsonProperty("type") Type type,
            @JsonProperty("argType") TypeToken<T> argType) {
        //noinspection unchecked
        return new AutoValue_ComparableArgumentExpression<>(type, (TypeToken<T>)argType.eliminateTypeVars());
    }
}
