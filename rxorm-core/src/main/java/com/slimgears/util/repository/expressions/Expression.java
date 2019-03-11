package com.slimgears.util.repository.expressions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.slimgears.util.repository.expressions.internal.*;

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
        And(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        Or(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        Not(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean),

        Equals(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        IsNull(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean),

        ValueIn(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),

        LessThan(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        GreaterThan(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),

        IsEmpty(BooleanUnaryOperationExpression.class, OperationType.Unary, ValueType.Boolean),
        Contains(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        StartsWith(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        EndsWith(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        Matches(BooleanBinaryOperationExpression.class, OperationType.Binary, ValueType.Boolean),
        Length(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric),
        Concat(StringBinaryOperationExpression.class, OperationType.Binary, ValueType.String),
        ToLower(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String),
        ToUpper(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String),
        Trim(StringUnaryOperationExpression.class, OperationType.Unary, ValueType.String),

        Negate(NumericUnaryOperationExpression.class, OperationType.Unary, ValueType.Numeric),
        Add(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),
        Sub(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),
        Mul(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),
        Div(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),

        Property(PropertyExpression.class, OperationType.Property, ValueType.Object),
        ComparableProperty(ComparablePropertyExpression.class, OperationType.Property, ValueType.Comparable),
        NumericProperty(ComparablePropertyExpression.class, OperationType.Property, ValueType.Numeric),
        StringProperty(StringPropertyExpression.class, OperationType.Property, ValueType.String),
        BooleanProperty(BooleanPropertyExpression.class, OperationType.Property, ValueType.Boolean),
        CollectionProperty(CollectionPropertyExpression.class, OperationType.Property, ValueType.Collection),

        Constant(ConstantExpression.class, OperationType.Constant, ValueType.Object),
        ComparableConstant(ComparableConstantExpression.class, OperationType.Constant, ValueType.Comparable),
        NumericConstant(NumericConstantExpression.class, OperationType.Constant, ValueType.Numeric),
        StringConstant(StringConstantExpression.class, OperationType.Constant, ValueType.String),
        BooleanConstant(BooleanConstantExpression.class, OperationType.Constant, ValueType.Boolean),
        CollectionConstant(CollectionConstantExpression.class, OperationType.Constant, ValueType.Collection),

        Composition(ObjectComposedExpression.class, OperationType.Constant, ValueType.Object),
        ComparableComposition(ComparableComposedExpression.class, OperationType.Constant, ValueType.Comparable),
        NumericComposition(NumericComposedExpression.class, OperationType.Constant, ValueType.Numeric),
        StringComposition(StringComposedExpression.class, OperationType.Constant, ValueType.String),
        BooleanComposition(BooleanComposedExpression.class, OperationType.Constant, ValueType.Boolean),
        CollectionComposition(CollectionComposedExpression.class, OperationType.Constant, ValueType.Collection),

        MapCollection(CollectionOperationExpression.class, OperationType.Collection, ValueType.Collection),
        FlatMapCollection(CollectionOperationExpression.class, OperationType.Collection, ValueType.Collection),
        FilterCollection(CollectionOperationExpression.class, OperationType.Collection, ValueType.Collection),

        Count(ObjectBinaryOperationExpression.class, OperationType.Binary, ValueType.Object),
        Min(ComparableBinaryOperationExpression.class, OperationType.Binary, ValueType.Comparable),
        Max(ComparableBinaryOperationExpression.class, OperationType.Binary, ValueType.Comparable),
        Average(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),
        Sum(NumericBinaryOperationExpression.class, OperationType.Binary, ValueType.Numeric),

        Argument(ObjectArgumentExpression.class, OperationType.Argument, ValueType.Object),
        CollectionArgument(CollectionArgumentExpression.class, OperationType.Argument, ValueType.Collection);

        Type(Class<? extends Expression> type, OperationType opType, ValueType valType) {
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

        @Override
        public String toString() {
            return modifyCase(super.toString(), Character::toLowerCase);
        }

        private static String modifyCase(String name, Function<Character, Character> modifier) {
            return Optional.ofNullable(name)
                    .filter(n -> !n.isEmpty())
                    .map(n -> modifier.apply(n.charAt(0)) + n.substring(1))
                    .orElse(name);
        }
    }
}
