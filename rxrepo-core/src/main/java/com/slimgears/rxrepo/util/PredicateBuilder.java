package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PredicateBuilder<S> {
    private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();

    public static <S> PredicateBuilder<S> create() {
        return new PredicateBuilder<>();
    }

    public PredicateBuilder<S> or(ObjectExpression<S, Boolean> predicate) {
        return combine(predicate, BooleanExpression::or);
    }

    public PredicateBuilder<S> and(ObjectExpression<S, Boolean> predicate) {
        return combine(predicate, BooleanExpression::and);
    }

    public PredicateBuilder<S> not() {
        return update(BooleanExpression::not);
    }

    public ObjectExpression<S, Boolean> build() {
        return predicate.get();
    }

    private PredicateBuilder<S> combine(ObjectExpression<S, Boolean> predicate, BiFunction<ObjectExpression<S, Boolean>, ObjectExpression<S, Boolean>, ObjectExpression<S, Boolean>> operator) {
        return update(p -> Optional
                        .ofNullable(p)
                        .map(pp -> operator.apply(pp, predicate))
                        .orElse(predicate));
    }

    private PredicateBuilder<S> update(Function<ObjectExpression<S, Boolean>, ObjectExpression<S, Boolean>> operator) {
        this.predicate.updateAndGet(p -> Optional.ofNullable(operator.apply(p)).orElse(p));
        return this;
    }
}
