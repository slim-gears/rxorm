package com.slimgears.rxrepo.filters;

import com.slimgears.rxrepo.annotations.FilterPrototype;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

@FilterPrototype
public interface CollectionFilterPrototype<T> extends ObjectFilterPrototype<Collection<T>> {
    @Nullable Boolean isEmpty();
    @Nullable T contains();
    @Nullable ComparableFilter<Integer> size();

    @Override
    default <S> Optional<BooleanExpression<S>> toExpression(ObjectExpression<S, Collection<T>> arg) {
        Supplier<CollectionExpression<S, T>> collectionExpression = () -> ObjectExpression.asCollection(arg);
        return Filters.combineExpressions(
                ObjectFilterPrototype.super.toExpression(arg),
                Optional.ofNullable(size()).flatMap(s -> s.toExpression(collectionExpression.get().size())),
                Optional.ofNullable(contains()).map(collectionExpression.get()::contains),
                Optional.ofNullable(isEmpty()).map(isEmpty -> isEmpty
                        ? collectionExpression.get().isEmpty()
                        : collectionExpression.get().isNotEmpty()));
    }
}
