package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.ComparablePropertyExpression;
import com.slimgears.rxrepo.expressions.internal.NumericPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.ObjectPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.StringPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.StringUnaryOperationExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public interface PropertyExpression<S, T, V> extends ObjectExpression<S, V> {
    @Override
    default TypeToken<? extends V> objectType() {
        return property().type();
    }

    @JsonProperty ObjectExpression<S, T> target();
    @JsonProperty PropertyMeta<T, ? extends V> property();

    default StringExpression<S> asString() {
        return StringUnaryOperationExpression.create(Type.AsString, this);
    }

    static <S, T, V> ObjectPropertyExpression<S, T, V> ofObject(ObjectExpression<S, T> target, PropertyMeta<T, ? extends V> property) {
        return ObjectPropertyExpression.create(Type.Property, target, property);
    }

    static <S, V> ObjectPropertyExpression<S, S, V> ofObject(PropertyMeta<S, ? extends V> property) {
        return ofObject(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }

    static <S, T, V extends Comparable<V>> ComparablePropertyExpression<S, T, V> ofComparable(ObjectExpression<S, T> target, PropertyMeta<T, ? extends V> property) {
        return ComparablePropertyExpression.create(Type.ComparableProperty, target, property);
    }

    static <S, V extends Comparable<V>> ComparablePropertyExpression<S, S, V> ofComparable(PropertyMeta<S, ? extends V> property) {
        return ofComparable(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }

    static <S, T> StringPropertyExpression<S, T> ofString(ObjectExpression<S, T> target, PropertyMeta<T, ? extends String> property) {
        return StringPropertyExpression.create(Type.StringProperty, target, property);
    }

    static <S> StringPropertyExpression<S, S> ofString(PropertyMeta<S, ? extends String> property) {
        return ofString(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }

    static <S, T> BooleanPropertyExpression<S, T> ofBoolean(ObjectExpression<S, T> target, PropertyMeta<T, ? extends Boolean> property) {
        return BooleanPropertyExpression.create(Type.BooleanProperty, target, property);
    }

    static <S> BooleanPropertyExpression<S, S> ofBoolean(PropertyMeta<S, ? extends Boolean> property) {
        return ofBoolean(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }

    static <S, T, V extends Number & Comparable<V>> NumericPropertyExpression<S, T, V> ofNumeric(ObjectExpression<S, T> target, PropertyMeta<T, ? extends V> property) {
        return NumericPropertyExpression.create(Type.NumericProperty, target, property);
    }

    static <S, V extends Number & Comparable<V>> NumericPropertyExpression<S, S, V> ofNumeric(PropertyMeta<S, ? extends V> property) {
        return ofNumeric(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }

    static <S, T, E> CollectionPropertyExpression<S, T, E> ofCollection(ObjectExpression<S, T> target, PropertyMeta<T, ? extends Collection<E>> property) {
        return CollectionPropertyExpression.create(Type.CollectionProperty, target, property);
    }

    static <S, E> CollectionPropertyExpression<S, S, E> ofCollection(PropertyMeta<S, ? extends Collection<E>> property) {
        return ofCollection(ObjectExpression.arg(property.declaringType().objectClass()), property);
    }
}
