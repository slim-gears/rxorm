package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.util.stream.Lazy;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

public class DefaultSqlExpressionGenerator implements SqlExpressionGenerator {
    private final Lazy<ExpressionTextGenerator> sqlGenerator;

    public DefaultSqlExpressionGenerator() {
        sqlGenerator = Lazy.of(this::createBuilder)
                .map(ExpressionTextGenerator.Builder::build);
    }

    protected ExpressionTextGenerator.Builder createBuilder() {
        return ExpressionTextGenerator.builder()
                .add(Expression.Type.IsNull, "(%s is null)")
                .add(Expression.Type.Add, "(%s + %s)")
                .add(Expression.Type.Sub, "(%s - %s)")
                .add(Expression.Type.Mul, "(%s * %s)")
                .add(Expression.Type.Div, "(%s / %s)")
                .add(Expression.Type.Negate, "(-%s)")
                .add(Expression.Type.And, "(%s and %s)")
                .add(Expression.Type.Or, "(%s or %s)")
                .add(Expression.Type.Not, "(not %s)")
                .add(Expression.Type.Equals, "(%s = %s)")
                .add(Expression.Type.GreaterThan, "(%s > %s)")
                .add(Expression.Type.LessThan, "(%s < %s)")
                .add(Expression.Type.IsEmpty, "(%s = '')")
                .add(Expression.Type.Contains, formatAndFixQuotes("(%s like '%%' + %s + '%%')"))
                .add(Expression.Type.StartsWith, formatAndFixQuotes("(%s like %s + '%%')"))
                .add(Expression.Type.EndsWith, formatAndFixQuotes("(%s like '%%' + %s)"))
                .add(Expression.Type.Matches, "(%s like %s)")
                .add(Expression.Type.Length, "LEN(%s)")
                .add(Expression.Type.Concat, "(%s + %s)")
                .add(Expression.Type.ToLower, "LOWER(%s)")
                .add(Expression.Type.ToUpper, "UPPER(%s)")
                .add(Expression.Type.Trim, "TRIM(%s)")
                .add(Expression.Type.Count, "COUNT(%s)")
                .add(Expression.Type.Average, "AVERAGE(%s)")
                .add(Expression.Type.Min, "MIN(%s)")
                .add(Expression.Type.Max, "MAX(%s)")
                .add(Expression.Type.Sum, "SUM(%s)")
                .add(Expression.Type.StringConstant, "'%s'")
                .add(Expression.Type.AsString, "CONVERT(varchar, %s)")
                .add(Expression.Type.AsBoolean, "%s")
                .add(Expression.Type.AsNumeric, "%s")
                .add(Expression.Type.AsComparable, "%s")
                .add(Expression.Type.SearchText, notSupported())
                .add(Expression.OperationType.Argument, "__argument__")
                .add(Expression.OperationType.Constant, "%s")
                .add(Expression.OperationType.Property, ExpressionTextGenerator.Reducer.join("."))
                .add(Expression.OperationType.Composition, "")
                .add(Expression.ValueType.Null, "null");
    }

    public <T> T withParams(List<Object> params, Callable<T> callable) {
        return sqlGenerator.get().withInterceptor(createInterceptor().combineWith(paramsInterceptor(params)), callable);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression) {
        return sqlGenerator.get().generate(expression);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        return sqlGenerator.get().generate(expression, arg);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, List<Object> params) {
        return withParams(params, () -> toSqlExpression(expression));
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg, List<Object> params) {
        return withParams(params, () -> toSqlExpression(expression, arg));
    }

    protected static ExpressionTextGenerator.Reducer notSupported() {
        return (exp, str) -> {
            throw new IllegalArgumentException("Not supported expression");
        };
    }

    protected ExpressionTextGenerator.Interceptor createInterceptor() {
        return ExpressionTextGenerator.Interceptor.empty();
    }

    private static ExpressionTextGenerator.Reducer formatAndFixQuotes(String format) {
        return ExpressionTextGenerator.Reducer.fromFormat(format).andThen(str -> str.replaceAll("' \\+ '", ""));
    }

    private static ExpressionTextGenerator.Interceptor paramsInterceptor(List<Object> params) {
        return ExpressionTextGenerator.Interceptor.ofType(ConstantExpression.class, (visitor, expression, visitSupplier) -> {
            params.add(expression.value());
            return "?";
        });
    }
}
