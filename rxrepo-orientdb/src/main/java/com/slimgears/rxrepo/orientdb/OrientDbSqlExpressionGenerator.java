package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.sql.PropertyMetas;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.reflect.TypeToken;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.slimgears.rxrepo.sql.StatementUtils.concat;

public class OrientDbSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    private final ExpressionTextGenerator.Interceptor searchTextInterceptor = ExpressionTextGenerator.Interceptor.builder()
            .intercept(Expression.Type.Equals, ExpressionTextGenerator.Interceptor.ofType(BooleanBinaryOperationExpression.class, this::onVisitPropertyEquals))
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

    private String onVisitPropertyEquals(Function<? super ObjectExpression<?, ?>, String> visitor, BooleanBinaryOperationExpression<?, ?, ?> expression, Supplier<String> visitedExpression) {
        if (expression.left() instanceof PropertyExpression && expression.right() instanceof ConstantExpression) {
            return onVisitPropertyEquals(visitor, (PropertyExpression<?, ?, ?>)expression.left(), (ConstantExpression<?, ?>)expression.right(), visitedExpression);
        } else if (expression.right() instanceof PropertyExpression && expression.left() instanceof ConstantExpression) {
            return onVisitPropertyEquals(visitor, (PropertyExpression<?, ?, ?>)expression.right(), (ConstantExpression<?, ?>)expression.left(), visitedExpression);
        } else {
            return visitedExpression.get();
        }
    }

    private String onVisitPropertyEquals(Function<? super ObjectExpression<?, ?>, String> visitor, PropertyExpression<?, ?, ?> propertyExpression, ConstantExpression<?, ?> constExpression, Supplier<String> visitedExpression) {
        if (!PropertyMetas.isIndexableByString(propertyExpression.property())) {
            return visitedExpression.get();
        }

        Object value = constExpression.value();
        if (!(value instanceof HasMetaClass)) {
            return visitedExpression.get();
        }

        return concat(propertyExpression.property().name() + "AsString", "=", this.fromConstant(value.toString()));
    }

    private String searchTextToWildcard(String searchText) {
        return Arrays
                .stream(searchText.split("\\s"))
                .map(t -> "+" + t)
                .collect(Collectors.joining(" "));
    }
}
