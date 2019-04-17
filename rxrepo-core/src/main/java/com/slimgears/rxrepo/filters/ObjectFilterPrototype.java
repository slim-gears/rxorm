package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Optional;

@FilterPrototype
public interface ObjectFilterPrototype<T> extends Filter<T> {
    @Nullable Boolean isNull();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, T> arg) {
        return Optional
                .ofNullable(isNull())
                .map(isNull -> isNull ? arg.isNull() : arg.isNotNull());
    }
}
