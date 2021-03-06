package com.slimgears.rxrepo.query.provider;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.HasSelf;

public interface HasSortingInfo<T> {
    ImmutableList<SortingInfo<T, ?, ? extends Comparable<?>>> sorting();

    @AutoValuePrototype.Builder
    interface Builder<T, _B extends Builder<T, _B>> extends HasSelf<_B> {
        ImmutableList.Builder<SortingInfo<T, ?, ? extends Comparable<?>>> sortingBuilder();

        default <V extends Comparable<V>> _B sortAscending(PropertyExpression<T, ?, V> property) {
            sortingBuilder().add(SortingInfo.create(property, true));
            return self();
        }

        default <V extends Comparable<V>> _B sortDescending(PropertyExpression<T, ?, V> property) {
            sortingBuilder().add(SortingInfo.create(property, false));
            return self();
        }
    }
}
