package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import com.slimgears.util.generic.MoreStrings;

public abstract class AbstractUnaryOperationExpression<S, T, R> implements UnaryOperationExpression<S, T, R> {
    @Override
    public Reflect<S, R> reflect() {
        return new AbstractReflect<S, R>(this) {
            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitUnary(AbstractUnaryOperationExpression.this);
            }

            @Override
            public ObjectExpression<S, R> convert(Converter converter) {
                ObjectExpression<S, T> operand = operand().reflect().convert(converter);
                return converter.convert(operand != operand()
                    ? AbstractUnaryOperationExpression.this
                    : ObjectUnaryOperationExpression.create(type(), operand));
            }
        };
    }

    @Override
    public String toString() {
        return MoreStrings.format("{}({})", type().name(), operand());
    }
}
