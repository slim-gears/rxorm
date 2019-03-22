package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;

public class SqlExpressionGenerator {
    private final static ExpressionTextGenerator sqlGenerator = ExpressionTextGenerator.builder()
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
            .add(Expression.Type.Contains, formatAndFixQuotes("(%s like '%%'%s'%%')"))
            .add(Expression.Type.StartsWith, formatAndFixQuotes("(%s like %s'%%')"))
            .add(Expression.Type.EndsWith, formatAndFixQuotes("(%s like '%%'%s)"))
            .add(Expression.Type.Matches, "(%s like %s)")
            .add(Expression.Type.Length, "LEN(%s)")
            .add(Expression.Type.Concat, "CONCAT(%s, %s)")
            .add(Expression.Type.ToLower, "LOWER(%s)")
            .add(Expression.Type.ToUpper, "UPPER(%s)")
            .add(Expression.Type.Trim, "TRIM(%s)")
            .add(Expression.Type.Count, "COUNT(%s)")
            .add(Expression.Type.Average, "AVERAGE(%s)")
            .add(Expression.Type.Min, "MIN(%s)")
            .add(Expression.Type.Max, "MAX(%s)")
            .add(Expression.Type.Sum, "SUM(%s)")
            .add(Expression.Type.StringConstant, "'%s'")
            .add(Expression.OperationType.Argument, "__argument__")
            .add(Expression.OperationType.Constant, "%s")
            .add(Expression.OperationType.Property, ExpressionTextGenerator.Reducer.join("."))
            .add(Expression.OperationType.Composition, "")
            .add(Expression.ValueType.Null, "null")
            .postProcess(exp -> exp.replaceAll("''", ""))
            .build();

    public static <S, T> String toSqlExpression(ObjectExpression<S, T> expression) {
        return sqlGenerator.generate(expression);
    }

    public static <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        return sqlGenerator.generate(expression, arg);
    }

    private static ExpressionTextGenerator.Reducer formatAndFixQuotes(String format) {
        return ExpressionTextGenerator.Reducer.fromFormat(format).andThen(str -> str.replaceAll("''", ""));
    }
}
