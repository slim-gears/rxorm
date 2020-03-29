package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.Collection;

@AutoValue
public abstract class CollectionPropertyExpression<S, T, E, C extends Collection<E>>
    extends AbstractPropertyExpression<S, T, C>
    implements CollectionExpression<S, E, C> {
    @JsonCreator
    public static <S, T, E, C extends Collection<E>> CollectionPropertyExpression<S, T, E, C> create(
            @JsonProperty("type") Type type,
            @JsonProperty("target") ObjectExpression<S, T> target,
            @JsonProperty("property") PropertyMeta<T, C> property) {
        return new AutoValue_CollectionPropertyExpression<>(type, target, property);
    }

    @Override
    protected ObjectExpression<S, C> createConverted(ObjectExpression<S, T> newTarget) {
        return create(type(), newTarget, property());
    }
}
