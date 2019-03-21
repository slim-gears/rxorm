package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanUnaryOperationExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.ComparablePropertyExpression;
import com.slimgears.rxrepo.expressions.internal.NumericPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.ObjectArgumentExpression;
import com.slimgears.rxrepo.expressions.internal.ObjectPropertyExpression;
import com.slimgears.rxrepo.expressions.internal.StringPropertyExpression;
import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public interface ObjectExpression<S, T> extends Expression<S> {
    default @JsonIgnore TypeToken<? extends T> objectType() {
        return type().resolveType(this);
    }

    default BooleanExpression<S> eq(ObjectExpression<S, T> value) {
        return BooleanBinaryOperationExpression.create(Type.Equals, this, value);
    }

    default BooleanExpression<S> eq(T value) {
        return eq(ConstantExpression.of(value));
    }

    default BooleanExpression<S> notEq(ObjectExpression<S, T> value) {
        return eq(value).not();
    }

    default BooleanExpression<S> notEq(T value) {
        return eq(value).not();
    }

    default BooleanExpression<S> isNull() {
        return BooleanUnaryOperationExpression.create(Type.IsNull, this);
    }

    default BooleanExpression<S> isNotNull() {
        return isNull().not();
    }

    default BooleanExpression<S> in(T... values) {
        return in(ConstantExpression.of(values));
    }

    default BooleanExpression<S> in(ObjectExpression<S, Collection<T>> values) {
        return BooleanBinaryOperationExpression.create(Type.ValueIn, this, values);
    }

    default BooleanExpression<S> in(Collection<T> values) {
        return in(ConstantExpression.of(values));
    }

    default <R> ObjectExpression<S, R> compose(ObjectExpression<T, R> expression) {
        return ComposedExpression.ofObject(this, expression);
    }

    default <R> CollectionExpression<S, R> compose(CollectionExpression<T, R> expression) {
        return ComposedExpression.ofCollection(this, expression);
    }

    default <R extends Comparable<R>> ComparableExpression<S, R> compose(ComparableExpression<T, R> expression) {
        return ComposedExpression.ofComparable(this, expression);
    }

    default <N extends Number & Comparable<N>> NumericExpression<S, N> compose(NumericExpression<T, N> expression) {
        return ComposedExpression.ofNumeric(this, expression);
    }

    default BooleanExpression<S> compose(BooleanExpression<T> expression) {
        return ComposedExpression.ofBoolean(this, expression);
    }

    default StringExpression<S> compose(StringExpression<T> expression) {
        return ComposedExpression.ofString(this, expression);
    }

    default <V> ObjectExpression<S, V> ref(ObjectPropertyExpression<S, T, V> expression) {
        return PropertyExpression.ofObject(this, expression.property());
    }

    default <V extends Comparable<V>> ComparableExpression<S, V> ref(ComparablePropertyExpression<?, T, V> expression) {
        return PropertyExpression.ofComparable(this, expression.property());
    }

    default <V extends Number & Comparable<V>> NumericExpression<S, V> ref(NumericPropertyExpression<?, T, V> expression) {
        return PropertyExpression.ofNumeric(this, expression.property());
    }

    default BooleanExpression<S> ref(BooleanPropertyExpression<?, T> expression) {
        return PropertyExpression.ofBoolean(this, expression.property());
    }

    default StringExpression<S> ref(StringPropertyExpression<?, T> expression) {
        return PropertyExpression.ofString(this, expression.property());
    }

    default <E> CollectionExpression<S, E> ref(CollectionPropertyExpression<?, T, E> expression) {
        return PropertyExpression.ofCollection(this, expression.property());
    }

    default BooleanExpression<S> matches(ObjectExpression<S, String> pattern) {
        return BooleanBinaryOperationExpression.create(Type.Matches, this, pattern);
    }

    default BooleanExpression<S> matches(String pattern) {
        return matches(ConstantExpression.of(pattern));
    }

    static <S> ObjectExpression<S, S> arg(TypeToken<S> type) {
        return ObjectArgumentExpression.create(Type.Argument, type);
    }

    static <S, T> ObjectExpression<S, T> indirectArg(TypeToken<T> type) {
        return ObjectArgumentExpression.create(Type.Argument, type);
    }

    static <S> ObjectExpression<S, S> arg(Class<S> type) {
        return arg(TypeToken.of(type));
    }

    static <S, T> ObjectExpression<S, T> indirectArg(Class<T> type) {
        return indirectArg(TypeToken.of(type));
    }
}
