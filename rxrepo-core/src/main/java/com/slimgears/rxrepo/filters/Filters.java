package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.expressions.BooleanExpression;

import java.util.Arrays;
import java.util.Optional;

public class Filters {
    @SafeVarargs
    public static <T> Optional<BooleanExpression<T>> combine(Optional<BooleanExpression<T>>... filters) {
        return Arrays.stream(filters)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce((a, b) -> a.and(b));
    }

    @SafeVarargs
    public static <T> Filter<T> combine(Filter<T>... filters) {
        return Arrays
                .stream(filters)
                .reduce(Filter::combineWith)
                .orElseGet(Filter::empty);
    }
}
