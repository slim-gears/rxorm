package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.HasMetaClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OrientDbSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    private final ExpressionTextGenerator.Interceptor searchTextInterceptor = ExpressionTextGenerator.Interceptor.builder()
            .intercept(Expression.OperationType.Binary, ExpressionTextGenerator.Interceptor.ofType(BooleanBinaryOperationExpression.class, this::onVisitBinaryExpression))
            .intercept(Expression.Type.SearchText, ExpressionTextGenerator.Interceptor.ofType(BooleanBinaryOperationExpression.class, this::onVisitSearchTextExpression))
            .build();

    @Override
    protected ExpressionTextGenerator.Builder createBuilder() {
        return super.createBuilder()
                .add(Expression.Type.AsString, "%s.asString()");
    }

    @Override
    protected ExpressionTextGenerator.Interceptor createInterceptor() {
        return searchTextInterceptor;
    }

    @SuppressWarnings("unchecked")
    private String onVisitSearchTextExpression(Function<? super ObjectExpression<?, ?>, String> visitor, BooleanBinaryOperationExpression<?, ?, String> expression, Supplier<String> visitedExpression) {
        TypeToken<?> argType = expression.left().objectType();
        String searchText = ((ConstantExpression<?, String>)expression.right()).value();
        String wildcard = searchTextToWildcard(searchText);
        return String.format("(search_index('" + OrientDbSchemaProvider.toClassName(argType) + ".textIndex', %s) = true)", visitor.apply(ConstantExpression.of(wildcard)));
    }

    private String onVisitBinaryExpression(Function<? super ObjectExpression<?, ?>, String> visitor, BooleanBinaryOperationExpression<?, ?, ?> expression, Supplier<String> visitedExpression) {
        if (!requiresAsStringMapping(expression)) {
            return visitedExpression.get();
        }

        String left = visitBinaryArgument(visitor, expression.left());
        String right = visitBinaryArgument(visitor, expression.right());

        return super.reduce(expression, left, right);
    }

    private boolean requiresAsStringMapping(BooleanBinaryOperationExpression<?, ?, ?> expression) {
        return (expression.left().type().operationType() == Expression.OperationType.Property ||
                expression.right().type().operationType() == Expression.OperationType.Property) &&
                PropertyMetas.isEmbedded(expression.left().objectType());
    }

    private String visitBinaryArgument(Function<? super ObjectExpression<?, ?>, String> visitor, ObjectExpression<?, ?> expression) {
        if (expression.type().operationType() == Expression.OperationType.Property) {
            return (visitor.apply(expression) + "`AsString`").replace("``", "");
        } else if (expression.type().operationType() == Expression.OperationType.Constant) {
            ConstantExpression<?, ?> constantExpression = (ConstantExpression<?, ?>)expression;
            Object value = constantExpression.value();
            TypeToken<?> valueType = constantExpression.objectType();
            if (PropertyMetas.isEmbedded(valueType)) {
                return visitor.apply(ConstantExpression.of(String.valueOf(value)));
            } else if (valueType.isSubtypeOf(Collection.class)) {
                value = ((Collection<?>)value)
                        .stream()
                        .map(val -> val instanceof HasMetaClass ? val.toString() : val)
                        .collect(ImmutableList.toImmutableList());
                return visitor.apply(ConstantExpression.of(value));
            }
        }

        return visitor.apply(expression);
    }

    private String searchTextToWildcard(String searchText) {
        return Arrays
                .stream(searchText.split("\\s"))
                .map(t -> "+" + t + "*")
                .collect(Collectors.joining(" "));
    }
}
