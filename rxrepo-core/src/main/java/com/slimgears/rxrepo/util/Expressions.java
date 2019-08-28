package com.slimgears.rxrepo.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.encoding.MetaClassSearchableFields;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class Expressions {
    @SuppressWarnings("unchecked")
    public static <S, T> Function<S, T> compile(ObjectExpression<S, T> exp) {
        return exp != null
                ? (Function<S, T>)new InternalVisitor().visit(exp, null)
                : (Function<S, T>)Function.identity();
    }

    public static <S, V extends Comparable<V>> Comparator<S> compileComparator(PropertyExpression<S, ?, V> property, boolean ascending) {
        return ascending ? Comparator.comparing(compile(property)) : Comparator.comparing(compile(property)).reversed();
    }

    public static <S, V extends Comparable<V>> Comparator<S> compileComparator(PropertyExpression<S, ?, V> property) {
        return compileComparator(property, true);
    }

    public static <S> Predicate<S> compilePredicate(ObjectExpression<S, Boolean> predicateExp) {
        return predicateExp != null
                ? compile(predicateExp)::apply
                : e -> true;
    }

    public static <S> io.reactivex.functions.Predicate<S> compileRxPredicate(ObjectExpression<S, Boolean> predicateExp) {
        return compilePredicate(predicateExp)::test;
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
    private static <T1, T2> Function<Function[], Function> fromShortCircuitAnd() {
        return funcs -> val -> (boolean)funcs[0].apply(val) && (boolean)funcs[1].apply(val);
    }

    @SuppressWarnings("unchecked")
    private static <T1, T2> Function<Function[], Function> fromShortCircuitOr() {
        return funcs -> val -> (boolean)funcs[0].apply(val) || (boolean)funcs[1].apply(val);
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
                .put(Expression.Type.AsString, fromUnary(o -> o != null ? o.toString() : null))
                .put(Expression.Type.Add, fromNumericBinary(add()))
                .put(Expression.Type.Sub, fromNumericBinary(subtract()))
                .put(Expression.Type.Mul, fromNumericBinary(multiply()))
                .put(Expression.Type.Div, fromNumericBinary(divide()))
                .put(Expression.Type.Negate, fromNumericUnary(negate()))
                .put(Expression.Type.And, fromShortCircuitAnd())
                .put(Expression.Type.Or, fromShortCircuitOr())
                .put(Expression.Type.Not, fromUnary(Boolean.FALSE::equals))
                .put(Expression.Type.Equals, fromBinary(Objects::equals))
                .put(Expression.Type.GreaterThan, Expressions.<Comparable, Comparable, Boolean>fromBinary((a, b) -> a != null && b != null && a.compareTo(b) > 0))
                .put(Expression.Type.LessThan, Expressions.<Comparable, Comparable, Boolean>fromBinary((a, b) -> a != null && b != null && a.compareTo(b) < 0))
                .put(Expression.Type.IsEmpty, fromUnary(Strings::isNullOrEmpty))
                .put(Expression.Type.Contains, Expressions.fromBinary(contains()))
                .put(Expression.Type.StartsWith, Expressions.fromBinary(startsWith()))
                .put(Expression.Type.EndsWith, Expressions.fromBinary(endsWith()))
                .put(Expression.Type.Matches, Expressions.fromBinary(matches()))
                .put(Expression.Type.Length, Expressions.fromUnary(length()))
                .put(Expression.Type.Concat, Expressions.fromBinary(concat()))
                .put(Expression.Type.ToLower, Expressions.<String, String>fromUnary(s -> s != null ? s.toLowerCase() : null))
                .put(Expression.Type.ToUpper, Expressions.<String, String>fromUnary(s -> s != null ? s.toUpperCase() : null))
                .put(Expression.Type.Trim, Expressions.<String, String>fromUnary(s -> s != null ? s.trim() : null))
                .put(Expression.Type.Count, Expressions.fromUnary(count()))
                .put(Expression.Type.Average, Expressions.fromUnary(average()))
                .put(Expression.Type.Min, Expressions.fromUnary(min()))
                .put(Expression.Type.Max, Expressions.fromUnary(max()))
                .put(Expression.Type.Sum, Expressions.fromUnary(sum()))
                .put(Expression.Type.SearchText, Expressions.fromBinary(searchText()))
                .put(Expression.Type.ValueIn, Expressions.fromBinary((Object obj, Collection<Object> collection) -> obj != null && collection != null && collection.contains(obj)))
                .put(Expression.Type.IsNull, Expressions.fromUnary(Objects::isNull))
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
            return target -> Optional
                    .ofNullable(target)
                    .flatMap(Optionals.ofType(propertyMeta.declaringType().asClass()))
                    .map(propertyMeta::getValue)
                    .orElse(null);
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

    private static <T> Function<Collection<T>, T> min() {
        return c -> Optional
                .ofNullable(c)
                .flatMap(cc -> cc.stream().min(Comparator.nullsFirst(Expressions::compare)))
                .orElse(null);
    }

    private static <T> Function<Collection<T>, T> max() {
        return c -> Optional
                .ofNullable(c)
                .flatMap(cc -> cc.stream().max(Comparator.nullsFirst(Expressions::compare)))
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static <T> int compare(T left, T right) {
        return left instanceof Comparable && right instanceof Comparable
                ? ((Comparable<T>)left).compareTo(right)
                : left.toString().compareTo(right.toString());
    }

    private static Function<Collection<?>, Long> count() {
        return c -> Optional.ofNullable(c).map(cc -> (long)cc.size()).orElse(0L);
    }

    private static <N extends Number> Function<Collection<N>, Double> average() {
        Function<Collection<N>, N> sumFunc = sum();
        Function<Collection<?>, Long> countFunc = count();
        return col -> {
            long count = countFunc.apply(col);
            return count > 0 ? sumFunc.apply(col).doubleValue() / count : 0.0;
        };
    }

    private static <N extends Number> Function<Collection<N>, N> sum() {
        return col -> Optional
                .ofNullable(col)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .reduce(GenericMath::add)
                .orElse(null);
    }

    private static BiFunction<String, String, String> concat() {
        return (s1, s2) -> getStringOrEmpty(s1) + getStringOrEmpty(s2);
    }

    private static Function<String, Integer> length() {
        return s -> Optional.ofNullable(s).map(String::length).orElse(0);
    }

    private static <N extends Number> Function<N, N> negate() {
        return num -> num != null ? GenericMath.negate(num) : null;
    }

    private static BiFunction<Number, Number, Number> divide() {
        return numericBinariesWithDefaultNumbers(GenericMath::divide, 0, 1);
    }

    private static BiFunction<Number, Number, Number> multiply() {
        return numericBinariesWithDefaultNumbers(GenericMath::multiply, 0, 0);
    }

    private static BiFunction<Number, Number, Number> subtract() {
        return numericBinariesWithDefaultNumbers(GenericMath::subtract, 0, 0);
    }

    private static BiFunction<Number, Number, Number> add() {
        return numericBinariesWithDefaultNumbers(GenericMath::add, 0, 0);
    }

    private static BiFunction<String, String, Boolean> startsWith() {
        return (s1, s2) -> s1 != null && (s2 == null || s1.startsWith(s2));
    }

    private static BiFunction<String, String, Boolean> endsWith() {
        return (s1, s2) -> s1 != null && (s2 == null || s1.endsWith(s2));
    }

    private static BiFunction<String, String, Boolean> matches() {
        return (s1, s2) -> (s1 == null && s2 == null) || (s1 != null && s2 != null && s1.matches(s2));
    }

    private static <N extends Number> BiFunction<N, N, N> numericBinariesWithDefaultNumbers(BiFunction<N, N, N> func, N defaultValue1, N defaultValue2) {
        return (n1, n2) -> func.apply(getNumberOrDefault(n1, defaultValue1), getNumberOrDefault(n2, defaultValue2));
    }

    private static <N extends Number> N getNumberOrDefault(N num, N defaultValue){
        return Optional.ofNullable(num).orElse(defaultValue);
    }

    private static String getStringOrEmpty(String s){
        return Optional.ofNullable(s).orElse("");
    }

    private static BiFunction<Object, String, Boolean> searchText() {
        return (obj, str) -> Optional.ofNullable(obj)
                .map(MetaClassSearchableFields::searchableTextFromObject)
                .map(text -> {
                    Pattern pattern = Pattern.compile(SearchTextUtils.searchTextToRegex(getStringOrEmpty(str)), Pattern.CASE_INSENSITIVE);
                    return pattern.matcher(text).find();
                })
                .orElse(false);
    }

    private static BiFunction<String, String, Boolean> contains() {
        return (s1, s2) -> s1 != null && (s2 == null || s1.contains(s2)); //if s1 == null returns false but then null does not contain null
    }
}
