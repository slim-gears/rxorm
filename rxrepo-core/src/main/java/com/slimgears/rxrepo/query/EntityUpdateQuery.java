package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import io.reactivex.Single;

import java.util.Collection;

public interface EntityUpdateQuery<S> extends QueryBuilder<EntityUpdateQuery<S>, S> {

    <T extends HasMetaClass<T>, V> EntityUpdateQuery<S> set(PropertyExpression<S, T, V> property, ObjectExpression<S, V> value);
    <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> add(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item);
    <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> remove(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item);

    default <T extends HasMetaClass<T>, V> EntityUpdateQuery<S> set(PropertyExpression<S, T, V> property, V value) {
        return set(property, ConstantExpression.of(value));
    }

    default <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> add(CollectionPropertyExpression<S, T, V, C> property, V item) {
        return add(property, ConstantExpression.of(item));
    }

    default <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> remove(CollectionPropertyExpression<S, T, V, C> property, V item) {
        return remove(property, ConstantExpression.of(item));
    }

    Single<Integer> execute();
}
