package com.slimgears.rxrepo.util;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExpressionTextGenerator {
    private static <T> T[] requireArgs(T[] args, int count) {
        if (args.length < count) {
            throw new IllegalArgumentException("Expected " + count + " arguments, actual: " + args.length);
        }
        return args;
    }

    public interface Reducer {
        String reduce(String... parts);

        static Reducer fromFormat(String format) {
            return args -> String.format(format, (Object[])args);
        }

        static Reducer fromBinary(BiFunction<String, String, String> reducer) {
            return args -> reducer.apply(requireArgs(args, 2)[0], args[1]);
        }

        static Reducer fromUnary(Function<String, String> reducer) {
            return args -> reducer.apply(requireArgs(args, 1)[0]);
        }

        static Reducer just(String str) {
            return args -> str;
        }

        static Reducer join(String delimiter) {
            return args -> Arrays
                    .stream(args)
                    .filter(a -> !a.isEmpty())
                    .collect(Collectors.joining(delimiter));
        }

        default Reducer andThen(Function<String, String> postProcess) {
            return args -> postProcess.apply(this.reduce(args));
        }
    }

    private final ImmutableMap<Expression.Type, Reducer> typeToReducer;
    private final ImmutableMap<Expression.OperationType, Reducer> opTypeToReducer;
    private final ImmutableMap<Expression.ValueType, Reducer> valueTypeToReducer;

    public static Builder builder() {
        return new Builder();
    }

    private ExpressionTextGenerator(ImmutableMap<Expression.Type, Reducer> typeToReducer, ImmutableMap<Expression.OperationType, Reducer> opTypeToReducer, ImmutableMap<Expression.ValueType, Reducer> valueTypeToReducer, Function<String, String> postProcessor) {
        this.typeToReducer = typeToReducer;
        this.opTypeToReducer = opTypeToReducer;
        this.valueTypeToReducer = valueTypeToReducer;
    }

    public static class Builder {
        private final ImmutableMap.Builder<Expression.Type, Reducer> typeToReducerBuilder = ImmutableMap.builder();
        private final ImmutableMap.Builder<Expression.OperationType, Reducer> opTypeToReducerBuilder = ImmutableMap.builder();
        private final ImmutableMap.Builder<Expression.ValueType, Reducer> valTypeToReducerBuilder = ImmutableMap.builder();
        private final AtomicReference<Function<String, String>> postProcessor = new AtomicReference<>(Functions.identity());

        public Builder add(Expression.Type type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.Type type, Reducer reducer) {
            typeToReducerBuilder.put(type, reducer);
            return this;
        }

        public Builder add(Expression.OperationType type, Reducer reducer) {
            opTypeToReducerBuilder.put(type, reducer);
            return this;
        }

        public Builder add(Expression.OperationType type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.ValueType type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.ValueType type, Reducer reducer) {
            valTypeToReducerBuilder.put(type, reducer);
            return this;
        }

        public Builder postProcess(Function<String, String> processor) {
            postProcessor.updateAndGet(func -> func.andThen(processor));
            return this;
        }

        public ExpressionTextGenerator build() {
            return new ExpressionTextGenerator(
                    typeToReducerBuilder.build(),
                    opTypeToReducerBuilder.build(),
                    valTypeToReducerBuilder.build(),
                    postProcessor.get());
        }
    }


    public <S, T> String generate(ObjectExpression<S, T> expression) {
        return generate(expression, "");
    }
    
    public <S, T> String generate(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        String argStr = generate(arg);
        return generate(expression, argStr);
    }
    
    private String generate(ObjectExpression<?, ?> expression, String arg) {
        Visitor visitor = new Visitor();
        return visitor.visit(expression, arg);
    }

    private Reducer toReducer(Expression.Type type) {
        return Optionals.or(
                () -> reducerFromExpressionType(type),
                () -> reducerFromOperationType(type.operationType()),
                () -> reducerFromValueType(type.valueType()))
                .orElseGet(() -> Reducer.just("%s"));
    }

    private Optional<Reducer> reducerFromExpressionType(Expression.Type type) {
        return Optional.ofNullable(typeToReducer.get(type));
    }

    private Optional<Reducer> reducerFromValueType(Expression.ValueType type) {
        return Optional.ofNullable(valueTypeToReducer.get(type));
    }

    private Optional<Reducer> reducerFromOperationType(Expression.OperationType type) {
        return Optional.ofNullable(opTypeToReducer.get(type));
    }

    class Visitor extends ExpressionVisitor<String, String> {
        @Override
        protected String reduceBinary(Expression.Type type, String first, String second) {
            return toReducer(type).reduce(first, second);
        }

        @Override
        protected String reduceUnary(Expression.Type type, String first) {
            return toReducer(type).reduce(first);
        }

        @Override
        protected <S, T> String visitOther(ObjectExpression<S, T> expression, String context) {
            return toReducer(expression.type()).reduce();
        }

        @Override
        protected <S, T, R> String visitUnaryOperator(UnaryOperationExpression<S, T, R> expression, String context) {
            return super.visitUnaryOperator(expression, context);
        }

        @Override
        protected <T, V> String visitProperty(PropertyMeta<T, V> propertyMeta, String context) {
            return propertyMeta.name();
        }

        @Override
        protected <T> String visitArgument(TypeToken<T> argType, String context) {
            return context;
        }

        @Override
        protected <V> String visitConstant(Expression.Type type, V value, String context) {
            if (value instanceof String) {
                return reduceUnary(Expression.Type.StringConstant, (String)value);
            } else if (value == null) {
                return reduceUnary(Expression.Type.NullConstant, null);
            } else if (value instanceof Number) {
                return reduceUnary(Expression.Type.NumericConstant, String.valueOf(value));
            } else {
                return reduceUnary(type, String.valueOf(value));
            }
        }
    }
}
