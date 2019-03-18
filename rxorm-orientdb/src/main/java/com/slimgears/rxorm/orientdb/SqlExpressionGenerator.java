package com.slimgears.rxorm.orientdb;

import com.google.common.collect.ImmutableMap;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.repository.expressions.Expression;
import com.slimgears.util.repository.expressions.ExpressionVisitor;
import com.slimgears.util.repository.expressions.ObjectExpression;
import com.slimgears.util.repository.expressions.PropertyExpression;
import com.slimgears.util.repository.expressions.UnaryOperationExpression;
import com.slimgears.util.stream.Optionals;

import java.util.Optional;

public class SqlExpressionGenerator extends ExpressionVisitor<SqlExpressionGenerator.Context, String> {
    private final static ImmutableMap<Expression.Type, String> expressionTypeFormatMap = ImmutableMap.<Expression.Type, String>builder()
            .put(Expression.Type.Add, "(%s + %s)")
            .put(Expression.Type.Sub, "(%s - %s)")
            .put(Expression.Type.Mul, "(%s * %s)")
            .put(Expression.Type.Div, "(%s / %s)")
            .put(Expression.Type.Negate, "(-%s)")
            .put(Expression.Type.And, "(%s and %s)")
            .put(Expression.Type.Or, "(%s or %s)")
            .put(Expression.Type.Not, "(not %s)")
            .put(Expression.Type.Equals, "(%s = %s)")
            .put(Expression.Type.GreaterThan, "(%s > %s)")
            .put(Expression.Type.LessThan, "(%s < %s)")
            .put(Expression.Type.IsEmpty, "(%s = '')")
            .put(Expression.Type.Contains, "(%s like '%%'%s'%%')")
            .put(Expression.Type.StartsWith, "(%s like %s'%%')")
            .put(Expression.Type.EndsWith, "(%s like '%%'%s)")
            .put(Expression.Type.Matches, "(%s like '%%'%s)")
            .put(Expression.Type.Length, "LEN(%s)")
            .put(Expression.Type.Concat, "CONCAT(%s, %s)")
            .put(Expression.Type.ToLower, "LOWER(%s)")
            .put(Expression.Type.ToUpper, "UPPER(%s)")
            .put(Expression.Type.Trim, "TRIM(%s)")
            .put(Expression.Type.Count, "COUNT(%s)")
            .put(Expression.Type.Average, "AVERAGE(%s)")
            .put(Expression.Type.Min, "MIN(%s)")
            .put(Expression.Type.Max, "MAX(%s)")
            .put(Expression.Type.Sum, "SUM(%s)")
            .build();

    private final static ImmutableMap<Expression.OperationType, String> operationTypeFormatMap = ImmutableMap.<Expression.OperationType, String>builder()
            .put(Expression.OperationType.Argument, "__argument__")
            .put(Expression.OperationType.Constant, "%s")
            .put(Expression.OperationType.Property, "%s.%s")
            .put(Expression.OperationType.Composition, "")
            .build();

    private final static ImmutableMap<Expression.ValueType, String> valueTypeFormatMap = ImmutableMap.<Expression.ValueType, String>builder()
            .put(Expression.ValueType.Boolean, "%s")
            .put(Expression.ValueType.Numeric, "%s")
            .put(Expression.ValueType.String, "'%s'")
            .build();

    static class Context {
        private final String arg;

        Context(String arg) {
            this.arg = arg;
        }
    }

    private String toFormat(Expression.Type type) {
        return Optionals.or(
                () -> Optional.ofNullable(expressionTypeFormatMap.get(type)),
                () -> Optional.ofNullable(operationTypeFormatMap.get(type.operationType())),
                () -> Optional.ofNullable(valueTypeFormatMap.get(type.valueType())))
                .orElse("%s");
    }

    private static <S, T> String toSqlExpression(ObjectExpression<S, T> expression, String arg) {
        String exp = new SqlExpressionGenerator().visit(expression, new Context(arg));
        exp = exp.replaceAll("''", "");
        return exp;
    }

    public static <S, T> String toSqlExpression(ObjectExpression<S, T> expression) {
        return toSqlExpression(expression, "");
    }

    public static <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        String argStr = toSqlExpression(arg);
        return toSqlExpression(expression, argStr);
    }

    @Override
    protected String reduceBinary(Expression.Type type, String first, String second) {
        return String.format(toFormat(type), first, second);
    }

    @Override
    protected String reduceUnary(Expression.Type type, String first) {
        return String.format(toFormat(type), first);
    }

    @Override
    protected <S, T> String visitOther(ObjectExpression<S, T> expression, Context context) {
        return "";
    }

    @Override
    protected <S, T, R> String visitUnaryOperator(UnaryOperationExpression<S, T, R> expression, Context context) {
        return super.visitUnaryOperator(expression, context);
    }

    @Override
    protected <S, T, V> String visitProperty(PropertyExpression<S, T, V> expression, Context context) {
        String target = visit(expression.target(), context);
        if (!target.isEmpty()) {
            target = target + ".";
        }
        return target + visitProperty(expression.property(), context);
    }

    @Override
    protected <T, V> String visitProperty(PropertyMeta<T, V> propertyMeta, Context context) {
        return propertyMeta.name();
    }

    @Override
    protected <T> String visitArgument(TypeToken<T> argType, Context context) {
        return context.arg;
    }

    @Override
    protected <V> String visitConstant(V value, Context context) {
        return Optional.ofNullable(value)
                .map(val -> val instanceof String ? "'" + val + "'" : val.toString())
                .orElse("null");
    }
}
