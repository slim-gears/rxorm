package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.util.reflect.TypeToken;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OrientDbSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    private final ExpressionTextGenerator.Interceptor searchTextInterceptor = ExpressionTextGenerator.Interceptor.builder()
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

    private String searchTextToWildcard(String searchText) {
        return Arrays
                .stream(searchText.split("\\s"))
                .map(t -> "+" + t)
                .collect(Collectors.joining(" "));
    }
}
