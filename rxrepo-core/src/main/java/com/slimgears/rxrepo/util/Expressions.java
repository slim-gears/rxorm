package com.slimgears.rxrepo.util;

import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class Expressions {
    public static <S, T> Function<S, T> compile(ObjectExpression<S, T> exp) {
        //noinspection unchecked
        return (Function<S, T>)new InternalVisitor().visit(exp, null);
    }

    public static <S, V extends Comparable<V>> Comparator<S> compileComparator(PropertyExpression<S, ?, V> property, boolean ascending) {
        return ascending ? Comparator.comparing(compile(property)) : Comparator.comparing(compile(property)).reversed();
    }

    public static <S, V extends Comparable<V>> Comparator<S> compileComparator(PropertyExpression<S, ?, V> property) {
        return compileComparator(property, true);
    }

    public static <S> Predicate<S> compilePredicate(ObjectExpression<S, Boolean> predicateExp) {
        return compile(predicateExp)::apply;
    }

    public static <S> io.reactivex.functions.Predicate<S> compileRxPredicate(ObjectExpression<S, Boolean> predicateExp) {
        return compile(predicateExp)::apply;
    }

    public static <S, T> io.reactivex.functions.Function<S, T> compileRx(ObjectExpression<S, T> exp) {
        return compile(exp)::apply;
    }

    @SuppressWarnings("unchecked")
    private static <T, R> Function<Function[], Function> fromUnary(Function<T, R> func) {
        return funcs -> val -> func.apply((T)funcs[0].apply(val));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Number> Function<Function[], Function> fromNumericUnary(Function<T, T> func) {
        return funcs -> val -> func.apply((T)funcs[0].apply(val));
    }

    @SuppressWarnings("unchecked")
    private static <T1, T2, R> Function<Function[], Function> fromBinary(BiFunction<T1, T2, R> func) {
        return funcs -> val -> func.apply((T1)funcs[0].apply(val), (T2)funcs[1].apply(val));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Number> Function<Function[], Function> fromNumericBinary(BiFunction<T, T, T> func) {
        return funcs -> val -> func.apply((T)funcs[0].apply(val), (T)funcs[1].apply(val));
    }

    private static Function<Function[], Function> composition() {
        //noinspection unchecked
        return funcs -> Arrays.stream(funcs)
                .reduce((f1, f2) -> val -> f2.apply(f1.apply(val)))
                .orElse(val -> val);
    }

    private static Function<Function[], Function> notSupported() {
        return funcs -> {
            throw new IllegalArgumentException("Not supported operation");
        };
    }

    private static class InternalVisitor extends ExpressionVisitor<Void, Function> {
        @SuppressWarnings("unchecked")
        private final static ImmutableMap<Expression.Type, Function<Function[], Function>> expressionTypeReducersMap = ImmutableMap.<Expression.Type, Function<Function[], Function>>builder()
                .put(Expression.Type.AsString, fromUnary(Object::toString))
                .put(Expression.Type.Add, fromNumericBinary(GenericMath::add))
                .put(Expression.Type.Sub, fromNumericBinary(GenericMath::subtract))
                .put(Expression.Type.Mul, fromNumericBinary(GenericMath::multiply))
                .put(Expression.Type.Div, fromNumericBinary(GenericMath::divide))
                .put(Expression.Type.Negate, fromNumericUnary(GenericMath::negate))
                .put(Expression.Type.And, fromBinary(Boolean::logicalAnd))
                .put(Expression.Type.Or, fromBinary(Boolean::logicalOr))
                .put(Expression.Type.Not, fromUnary(Boolean.FALSE::equals))
                .put(Expression.Type.Equals, fromBinary(Object::equals))
                .put(Expression.Type.GreaterThan, Expressions.<Comparable, Comparable, Boolean>fromBinary((a, b) -> a.compareTo(b) > 0))
                .put(Expression.Type.LessThan, Expressions.<Comparable, Comparable, Boolean>fromBinary((a, b) -> a.compareTo(b) < 0))
                .put(Expression.Type.IsEmpty, fromUnary(String::isEmpty))
                .put(Expression.Type.Contains, fromBinary(String::contains))
                .put(Expression.Type.StartsWith, Expressions.<String, String, Boolean>fromBinary(String::startsWith))
                .put(Expression.Type.EndsWith, fromBinary(String::endsWith))
                .put(Expression.Type.Matches, fromBinary(String::matches))
                .put(Expression.Type.Length, fromUnary(String::length))
                .put(Expression.Type.Concat, Expressions.<String, String, String>fromBinary((s1, s2) -> s1 + s2))
                .put(Expression.Type.ToLower, Expressions.<String, String>fromUnary(String::toLowerCase))
                .put(Expression.Type.ToUpper, Expressions.<String, String>fromUnary(String::toUpperCase))
                .put(Expression.Type.Trim, fromUnary(String::trim))
                .put(Expression.Type.Count, Expressions.<Collection, Integer>fromUnary(Collection::size))
                .put(Expression.Type.Average, notSupported())
                .put(Expression.Type.Min, notSupported())
                .put(Expression.Type.Max, notSupported())
                .put(Expression.Type.Sum, notSupported())
                .put(Expression.Type.SearchText, Expressions.fromBinary((Object obj, String str) -> obj.toString().contains(str)))
                .put(Expression.Type.ValueIn, Expressions.fromBinary((Object obj, Collection<Object> collection) -> collection.contains(obj)))
                .build();

        private final static ImmutableMap<Expression.OperationType, Function<Function[], Function>> operationTypeReducersMap = ImmutableMap.<Expression.OperationType, Function<Function[], Function>>builder()
                .put(Expression.OperationType.Property, composition())
                .put(Expression.OperationType.Argument, funcs -> funcs[0])
                .put(Expression.OperationType.Constant, funcs -> funcs[0])
                .put(Expression.OperationType.Composition, composition())
                .build();

        private static Function reduce(Expression.Type type, Function... functions) {
            return Optionals.or(
                    () -> Optional.ofNullable(expressionTypeReducersMap.get(type)).map(r -> r.apply(functions)),
                    () -> Optional.ofNullable(operationTypeReducersMap.get(type.operationType())).map(r -> r.apply(functions)))
                    .orElseThrow(() -> new IllegalArgumentException("Not supported expression type: " + type));
        }

        @Override
        protected Function reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, Function first, Function second) {
            return reduce(type, first, second);
        }

        @Override
        protected Function reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, Function first) {
            return reduce(type, first);
        }

        @Override
        protected <S, T> Function visitOther(ObjectExpression<S, T> expression, Void arg) {
            throw new IllegalArgumentException("Not supported expression type: " + expression.toString());
        }

        @Override
        protected <T, V> Function visitProperty(PropertyMeta<T, V> propertyMeta, Void arg) {
            //noinspection unchecked
            return target -> propertyMeta.getValue((T)target);
        }

        @Override
        protected <V> Function visitConstant(Expression.Type type, V value, Void arg) {
            return a -> value;
        }

        @Override
        protected <T> Function visitArgument(TypeToken<T> argType, Void arg) {
            return a -> a;
        }
    }
}
