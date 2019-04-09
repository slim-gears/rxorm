package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.StringExpression;

import javax.annotation.Nullable;
import java.util.Optional;

@FilterPrototype
public interface StringFilterPrototype extends ComparableFilterPrototype<String> {
    @Nullable String contains();
    @Nullable String startsWith();
    @Nullable String endsWith();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, String> arg) {
        StringExpression<S> stringArg = ObjectExpression.asString(arg);
        return Filters.combine(
                ComparableFilterPrototype.super.toExpression(stringArg),
                Optional.ofNullable(contains()).map(stringArg::contains),
                Optional.ofNullable(startsWith()).map(stringArg::startsWith),
                Optional.ofNullable(endsWith()).map(stringArg::endsWith));
    }
}
