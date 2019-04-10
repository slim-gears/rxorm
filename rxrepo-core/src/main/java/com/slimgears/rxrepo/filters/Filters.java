package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Arrays;
import java.util.Optional;

public class Filters {
    public static <S, T> Optional<BooleanExpression<S>> fromTextFilter(SearchableFilter filter, ObjectExpression<S, T> arg) {
        return Optional.ofNullable(filter.searchText()).map(arg::searchText);
    }

    @SafeVarargs
    public static <T> Optional<BooleanExpression<T>> combineExpressions(Optional<BooleanExpression<T>>... filters) {
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
