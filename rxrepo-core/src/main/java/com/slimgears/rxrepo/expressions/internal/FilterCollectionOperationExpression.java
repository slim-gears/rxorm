package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.CollectionOperationExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Collection;

@AutoValue
public abstract class FilterCollectionOperationExpression<S, T, C extends Collection<T>>
    implements CollectionOperationExpression<S, T, Boolean, T, C, C> {
    @JsonCreator
    public static <S, T, C extends Collection<T>> FilterCollectionOperationExpression<S, T, C> create(
            @JsonProperty("type") Type type,
            @JsonProperty("source") ObjectExpression<S, C> source,
            @JsonProperty("operation") ObjectExpression<T, Boolean> operation) {
        return new AutoValue_FilterCollectionOperationExpression<>(type, source, operation);
    }

    @Override
    public Reflect<S, C> reflect() {
        return new AbstractCollectionOperationReflect<S, T, Boolean, T, C, C>(this) {
            @SuppressWarnings("UnstableApiUsage")
            @Override
            public TypeToken<C> objectType() {
                return source().reflect().objectType();
            }

            @Override
            protected ObjectExpression<S, C> create(ObjectExpression<S, C> source, ObjectExpression<T, Boolean> operation) {
                return FilterCollectionOperationExpression.create(type(), source, operation);
            }
        };
    }
}
