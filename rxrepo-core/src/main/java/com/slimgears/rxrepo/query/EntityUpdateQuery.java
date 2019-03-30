package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;

public interface EntityUpdateQuery<K, S extends HasMetaClassWithKey<K, S>>
            extends QueryBuilder<EntityUpdateQuery<K, S>, K, S> {

    <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> set(PropertyExpression<S, T, V> property, ObjectExpression<S, V> value);
    <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> add(CollectionPropertyExpression<S, T, V> property, ObjectExpression<S, V> item);
    <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> remove(CollectionPropertyExpression<S, T, V> property, ObjectExpression<S, V> item);

    default <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> set(PropertyExpression<S, T, V> property, V value) {
        return set(property, ConstantExpression.of(value));
    }

    default <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> add(CollectionPropertyExpression<S, T, V> property, V item) {
        return add(property, ConstantExpression.of(item));
    }

    default <T extends HasMetaClass<T>, V> EntityUpdateQuery<K, S> remove(CollectionPropertyExpression<S, T, V> property, V item) {
        return remove(property, ConstantExpression.of(item));
    }

    Observable<S> execute();
}
