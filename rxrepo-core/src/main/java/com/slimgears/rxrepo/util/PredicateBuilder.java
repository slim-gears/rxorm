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
        if (predicate != null) {
            return update(p -> operator.apply(p, predicate));
        }
        return this;
    }

    private PredicateBuilder<S> update(Function<ObjectExpression<S, Boolean>, ObjectExpression<S, Boolean>> operator) {
        this.predicate.updateAndGet(p -> Optional
                .ofNullable(p)
                .map(operator)
                .orElse(p));
        return this;
    }
}
