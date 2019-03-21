package com.slimgears.rxrepo.expressions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.slimgears.rxrepo.expressions.internal.*;
import com.slimgears.util.reflect.TypeToken;

import java.util.Optional;
import java.util.function.Function;

@JsonTypeIdResolver(ExpressionTypeResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
public interface Expression<S> {
    Type type();

    enum ValueType {
        Object,
        Boolean,
        Comparable,
        Numeric,
        String,
        Collection
    }

    enum OperationType {
        Constant,
        Argument,
        Property,
        Unary,
        Binary,
        Collection,
        Composition
    }

    enum Type {
        And(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        Or(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        Not(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean, just(Boolean.class)),

        Equals(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        IsNull(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean, just(Boolean.class)),

        ValueIn(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),

        LessThan(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        GreaterThan(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),

        IsEmpty(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean, just(Boolean.class)),
        Contains(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        StartsWith(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        EndsWith(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        Matches(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean, just(Boolean.class)),
        Length(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric, just(Integer.class)),
        Concat(StringBinaryOperationExpression.class, OperationType.Binary, ValueType.String, just(String.class)),
        ToLower(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String, just(String.class)),
        ToUpper(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String, just(String.class)),
        Trim(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String, just(String.class)),

        Negate(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric, Type::fromArgument),
        Add(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric, Type::fromFirstArgument),
        Sub(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric, Type::fromFirstArgument),
        Mul(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric, Type::fromFirstArgument),
        Div(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric, Type::fromFirstArgument),

        Property(PropertyExpression.class, OperationType.Property, ValueType.Object, Type::fromProperty),
        ComparableProperty(ComparablePropertyExpression.class, OperationType.Property, ValueType.Comparable, Type::fromProperty),
        NumericProperty(ComparablePropertyExpression.class, OperationType.Property, ValueType.Numeric, Type::fromProperty),
        StringProperty(StringPropertyExpression.class, OperationType.Property, ValueType.String, Type::fromProperty),
        BooleanProperty(BooleanPropertyExpression.class, OperationType.Property, ValueType.Boolean, Type::fromProperty),
        CollectionProperty(CollectionPropertyExpression.class, OperationType.Property, ValueType.Collection, Type::fromProperty),

        Constant(ConstantExpression.class, OperationType.Constant, ValueType.Object, Type::fromConstant),
        ComparableConstant(ComparableConstantExpression.class, OperationType.Constant, ValueType.Comparable, Type::fromConstant),
        NumericConstant(NumericConstantExpression.class, OperationType.Constant, ValueType.Numeric, Type::fromConstant),
        StringConstant(StringConstantExpression.class, OperationType.Constant, ValueType.String, Type::fromConstant),
        BooleanConstant(BooleanConstantExpression.class, OperationType.Constant, ValueType.Boolean, Type::fromConstant),
        CollectionConstant(CollectionConstantExpression.class, OperationType.Constant, ValueType.Collection, Type::fromConstant),

        Composition(ObjectComposedExpression.class, OperationType.Composition, ValueType.Object, Type::fromComposition),
        ComparableComposition(ComparableComposedExpression.class, OperationType.Composition, ValueType.Comparable, Type::fromComposition),
        NumericComposition(NumericComposedExpression.class, OperationType.Composition, ValueType.Numeric, Type::fromComposition),
        StringComposition(StringComposedExpression.class, OperationType.Composition, ValueType.String, Type::fromComposition),
        BooleanComposition(BooleanComposedExpression.class, OperationType.Composition, ValueType.Boolean, Type::fromComposition),
        CollectionComposition(CollectionComposedExpression.class, OperationType.Composition, ValueType.Collection, Type::fromComposition),

        MapCollection(MapCollectionOperationExpression.class, OperationType.Collection, ValueType.Collection, Type::overridden),
        FlatMapCollection(FlatMapCollectionOperationExpression.class, OperationType.Collection, ValueType.Collection, Type::overridden),
        FilterCollection(FilterCollectionOperationExpression.class, OperationType.Collection, ValueType.Collection, Type::overridden),

        Count(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric, just(Long.class)),
        Min(ComparableUnaryOperationExpression.class, OperationType.Unary, ValueType.Comparable, Type::fromArgument),
        Max(ComparableUnaryOperationExpression.class, OperationType.Unary, ValueType.Comparable, Type::fromArgument),
        Average(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric, just(Double.class)),
        Sum(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric, Type::fromArgument),

        Argument(ObjectArgumentExpression.class, OperationType.Argument, ValueType.Object, Type::overridden),
        CollectionArgument(CollectionArgumentExpression.class, OperationType.Argument, ValueType.Collection, Type::overridden);

        Type(Class<? extends Expression> type, OperationType opType, ValueType valType, Function<ObjectExpression<?, ?>, TypeToken<?>> typeResolver) {
            this.typeResolver = typeResolver;
            this.type = type;
            this.operationType = opType;
            this.valueType = valType;
        }

        public Class<? extends Expression> type() {
            return this.type;
        }

        public ValueType valueType() {
            return this.valueType;
        }

        public OperationType operationType() {
            return this.operationType;
        }

        @JsonCreator
        public static Type fromString(String key) {
            return Type.valueOf(modifyCase(key, Character::toUpperCase));
        }

        private final Class<? extends Expression> type;
        private final OperationType operationType;
        private final ValueType valueType;
        private final Function<ObjectExpression<?, ?>, TypeToken<?>> typeResolver;

        @Override
        public String toString() {
            return modifyCase(super.toString(), Character::toLowerCase);
        }

        public <S, T> TypeToken<T> resolveType(ObjectExpression<S, T> exp) {
            //noinspection unchecked
            return (TypeToken<T>)typeResolver.apply(exp);
        }

        private static String modifyCase(String name, Function<Character, Character> modifier) {
            return Optional.ofNullable(name)
                    .filter(n -> !n.isEmpty())
                    .map(n -> modifier.apply(n.charAt(0)) + n.substring(1))
                    .orElse(name);
        }

        private static Function<ObjectExpression<?, ?>, TypeToken<?>> just(Class<?> cls) {
            TypeToken<?> typeToken = TypeToken.of(cls);
            return exp -> typeToken;
        }

        private static <S, T> TypeToken<? extends T> fromArgument(ObjectExpression<S, T> exp) {
            return requireInstanceOf(exp, new TypeToken<UnaryOperationExpression<S, T, T>>(){})
                    .operand()
                    .objectType();
        }

        private static <S, T> TypeToken<? extends T> fromFirstArgument(ObjectExpression<S, T> exp) {
            return requireInstanceOf(exp, new TypeToken<BinaryOperationExpression<S, T, T, T>>(){})
                    .left()
                    .objectType();
        }

        private static <S, T> TypeToken<? extends T> fromProperty(ObjectExpression<S, T> exp) {
            return requireInstanceOf(exp, new TypeToken<PropertyExpression<S, ?, T>>(){})
                    .property()
                    .type();
        }

        private static <S, T> TypeToken<? extends T> fromConstant(ObjectExpression<S, T> exp) {
            //noinspection unchecked
            Class<T> cls = (Class<T>)requireInstanceOf(exp, new TypeToken<ConstantExpression<S, T>>(){})
                    .value()
                    .getClass();
            return TypeToken.of(cls);
        }

        private static <S, T> TypeToken<? extends T> fromComposition(ObjectExpression<S, T> exp) {
            return requireInstanceOf(exp, new TypeToken<ComposedExpression<S, ?, T>>(){})
                .expression()
                .objectType();
        }

        private static <S, T> TypeToken<? extends T> overridden(ObjectExpression<S, T> exp) {
            return exp.objectType();
        }

        private static <T, R extends T> R requireInstanceOf(T obj, TypeToken<R> typeToken) {
            if (!typeToken.asClass().isInstance(obj)) {
                throw new RuntimeException("Should be instance of: " + typeToken);
            }
            //noinspection unchecked
            return (R)obj;
        }
    }
}
