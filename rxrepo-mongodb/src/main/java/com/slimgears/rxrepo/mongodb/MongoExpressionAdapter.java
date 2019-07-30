package com.slimgears.rxrepo.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;
import org.bson.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.slimgears.rxrepo.mongodb.codecs.MetaClassCodec.fieldName;

class MongoExpressionAdapter extends ExpressionVisitor<Void, Object> {
    private final static Map<Class<?>, ImmutableList<String>> searchableFieldsPerClass = new ConcurrentHashMap<>();

    private final static ImmutableMap<Expression.Type, Reducer> expressionTypeReducers = ImmutableMap
            .<Expression.Type, Reducer>builder()
            .put(Expression.Type.IsNull, args -> expr("$eq", args[0], null))
            .put(Expression.Type.Add, args -> expr("$add", args))
            .put(Expression.Type.Sub, args -> expr("$subtract", args))
            .put(Expression.Type.Mul, args -> expr("$multiply", args))
            .put(Expression.Type.Div, args -> expr("$divide", args))
            .put(Expression.Type.Not, args -> expr("$not", args))
            .put(Expression.Type.And, args -> expr("$and", args))
            .put(Expression.Type.Or, args -> expr("$or", args))
            .put(Expression.Type.Equals, args -> expr("$eq", args))
            .put(Expression.Type.LessThan, args -> expr("$lt", args))
            .put(Expression.Type.GreaterThan, args -> expr("$gt", args))
            .put(Expression.Type.ValueIn, args -> expr("$in", args))
            .put(Expression.Type.Length, args -> expr("$strLenCP", args))
            .put(Expression.Type.StartsWith, args -> expr("$eq", expr("$indexOfCP", args), 0))
            .put(Expression.Type.SearchText, args -> reduce(Expression.Type.Contains, args[0] + "_text", args[1]))
            .put(Expression.Type.EndsWith, args -> expr("$eq",
                    expr("$indexOfCP", args),
                    expr("$subtract",
                            expr("$strLenCP", args[0]),
                            expr("$strLenCP", args[1]))))
            .put(Expression.Type.Contains, args -> expr("$gte", expr("$indexOfCP", args), 0))
            .put(Expression.Type.Count, args -> expr("$count", MongoPipeline.aggregationField))
            .put(Expression.Type.Min, args -> expr("$min", MongoPipeline.aggregationField))
            .put(Expression.Type.Max, args -> expr("$max", MongoPipeline.aggregationField))
            .put(Expression.Type.Sum, args -> expr("$sum", MongoPipeline.aggregationField))
            .put(Expression.Type.Average, args -> expr("$avg", MongoPipeline.aggregationField))
            .build();

    private final static ImmutableMap<Expression.OperationType, Reducer> operationTypeReducers = ImmutableMap
            .<Expression.OperationType, Reducer>builder()
            .put(Expression.OperationType.Property, MongoExpressionAdapter::reduceProperties)
            .build();

    private static Document expr(String operator, Object... args) {
        return new Document(operator, args.length == 1 ? args[0] : Arrays.asList(args));
    }

    private static Object reduce(Expression.Type type, Object... args) {
        return Optionals.or(
                () -> Optional.ofNullable(expressionTypeReducers.get(type)),
                () -> Optional.ofNullable(operationTypeReducers.get(type.operationType())))
                .map(r -> r.reduce(args))
                .orElseThrow(() -> new RuntimeException("Cannot reduce expression of type " + type));
    }

    private static Object reduceProperties(Object... parts) {
        String head = parts[0].toString();
        String tail = parts[1].toString();
        return head.equals("$") ? head + tail : head + "." + tail;
    }

    @Override
    protected Object reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, Object first, Object second) {
        return reduce(type, first, second);
    }

    @Override
    protected Object reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, Object first) {
        return reduce(type, first);
    }

    @Override
    protected <T, V> Object visitProperty(PropertyMeta<T, V> propertyMeta, Void arg) {
        return fieldName(propertyMeta);
    }

    @Override
    protected <V> Object visitConstant(Expression.Type type, V value, Void arg) {
        return (value instanceof String && ((String)value).contains("$"))
                ? new Document().append("$literal", value)
                : value;
    }

    @Override
    protected <T> Object visitArgument(TypeToken<T> argType, Void arg) {
        return "$";
    }

    interface Reducer {
        Object reduce(Object... args);
    }
}
