package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.internal.*;

import java.util.Collection;

public interface ObjectExpression<S, T> extends Expression {
    @JsonIgnore default TypeToken<T> objectType() {
        return type().resolveType(this);
    }

    default BooleanExpression<S> eq(ObjectExpression<S, T> value) {
        return BooleanBinaryOperationExpression.create(Type.Equals, this, value);
    }

    default BooleanExpression<S> eq(T value) {
        return value != null
                ? eq(ConstantExpression.of(value))
                : isNull();
    }

    default BooleanExpression<S> notEq(ObjectExpression<S, T> value) {
        return eq(value).not();
    }

    default BooleanExpression<S> notEq(T value) {
        return value != null
                ? eq(value).not()
                : isNotNull();

    }

    default BooleanExpression<S> isNull() {
        return BooleanUnaryOperationExpression.create(Type.IsNull, this);
    }

    default BooleanExpression<S> isNotNull() {
        return isNull().not();
    }

    @SuppressWarnings("unchecked")
    default BooleanExpression<S> in(T... values) {
        return in(ConstantExpression.of(values));
    }

    default BooleanExpression<S> in(ObjectExpression<S, ? extends Collection<T>> values) {
        return BooleanBinaryOperationExpression.create(Type.ValueIn, this, values);
    }

    default BooleanExpression<S> in(Collection<T> values) {
        return in(ConstantExpression.of(values));
    }

    default <R> ObjectExpression<S, R> compose(ObjectExpression<T, R> expression) {
        return ComposedExpression.ofObject(this, expression);
    }

    default <R, C extends Collection<R>> CollectionExpression<S, R, C> compose(CollectionExpression<T, R, C> expression) {
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

    default <E, C extends Collection<E>> CollectionExpression<S, E, C> ref(CollectionPropertyExpression<?, T, E, C> expression) {
        return PropertyExpression.ofCollection(this, expression.property());
    }

    default BooleanExpression<S> searchText(String pattern) {
        return BooleanBinaryOperationExpression.create(Type.SearchText, this, ConstantExpression.of(pattern));
    }

    static <S> ObjectExpression<S, S> arg(TypeToken<S> type) {
        return ObjectArgumentExpression.create(Type.Argument, type);
    }

    static <S, T extends Comparable<T>> ComparableExpression<S, T> comparableArg(TypeToken<T> type) {
        return ComparableArgumentExpression.create(Type.ComparableArgument, type);
    }

    static <S, T extends Comparable<T>> ComparableExpression<S, T> comparableArg(Class<T> type) {
        return comparableArg(TypeToken.of(type));
    }

    static <S, T extends Number & Comparable<T>> NumericExpression<S, T> numericArg(TypeToken<T> type) {
        return NumericArgumentExpression.create(Type.NumericArgument, type);
    }

    static <S, T extends Number & Comparable<T>> NumericExpression<S, T> numericArg(Class<T> type) {
        return numericArg(TypeToken.of(type));
    }

    static <S> StringExpression<S> stringArg() {
        return StringArgumentExpression.create(Type.NumericArgument, TypeToken.of(String.class));
    }

    static <S> BooleanExpression<S> booleanArg() {
        return BooleanArgumentExpression.create(Type.BooleanArgument, TypeToken.of(Boolean.class));
    }

    static <S, T> ObjectExpression<S, T> objectArg(TypeToken<T> type) {
        return ObjectArgumentExpression.create(Type.Argument, type);
    }

    static <S> ObjectExpression<S, S> arg(Class<S> type) {
        return arg(TypeToken.of(type));
    }

    static <S, T> ObjectExpression<S, T> objectArg(Class<T> type) {
        return objectArg(TypeToken.of(type));
    }

    static <S, T extends Comparable<T>> ComparableExpression<S, T> asComparable(ObjectExpression<S, T> expression) {
        return expression instanceof ComparableExpression
                ? (ComparableExpression<S, T>)expression
                : ComparableUnaryOperationExpression.create(Type.AsComparable, expression);
    }

    static <S> StringExpression<S> asString(ObjectExpression<S, String> expression) {
        return expression instanceof StringExpression
                ? (StringExpression<S>)expression
                : StringUnaryOperationExpression.create(Type.AsString, expression);
    }

    static <S> BooleanExpression<S> asBoolean(ObjectExpression<S, Boolean> expression) {
        return expression instanceof BooleanExpression
                ? (BooleanExpression<S>)expression
                : BooleanUnaryOperationExpression.create(Type.AsBoolean, expression);
    }

    static <S, T, C extends Collection<T>> CollectionExpression<S, T, C> asCollection(ObjectExpression<S, C> expression) {
        return expression instanceof CollectionExpression
                ? (CollectionExpression<S, T, C>)expression
                : FilterCollectionOperationExpression.create(Type.CollectionFilter, expression, BooleanExpression.ofTrue());
    }

    static <S, N extends Number & Comparable<N>> NumericExpression<S, N> asNumeric(ObjectExpression<S, N> expression) {
        return expression instanceof NumericExpression
                ? (NumericExpression<S, N>)expression
                : NumericUnaryOperationExpression.create(Type.AsNumeric, expression);
    }
}
