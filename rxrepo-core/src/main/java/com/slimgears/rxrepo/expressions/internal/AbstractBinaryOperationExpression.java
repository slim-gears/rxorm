package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.BinaryOperationExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.generic.MoreStrings;

public abstract class AbstractBinaryOperationExpression<S, T1, T2, R>
    extends AbstractObjectExpression<S, R>
    implements BinaryOperationExpression<S, T1, T2, R> {

    @Override
    protected Reflect<S, R> createReflect() {
        return new AbstractReflect<S, R>(this) {
            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitBinary(AbstractBinaryOperationExpression.this);
            }

            @Override
            public ObjectExpression<S, R> convert(Converter converter) {
                ObjectExpression<S, T1> left = left().reflect().convert(converter);
                ObjectExpression<S, T2> right = right().reflect().convert(converter);
                return converter.convert((left == left() && right == right())
                    ? AbstractBinaryOperationExpression.this
                    : createConverted(left, right));
            }
        };
    }

    protected abstract ObjectExpression<S, R> createConverted(ObjectExpression<S, T1> newLeft, ObjectExpression<S, T2> newRight);

    @Override
    public String toString() {
        return MoreStrings.format("{}({}, {})", type().name(), left(), right());
    }
}
