package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

public abstract class AbstractConstantExpression<S, T> extends AbstractObjectExpression<S, T> implements ConstantExpression<S, T> {
    @Override
    protected Reflect<S, T> createReflect() {
        return new AbstractReflect<S, T>(this) {
            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitConstant(AbstractConstantExpression.this);
            }

            @Override
            public ObjectExpression<S, T> convert(Converter converter) {
                return converter.convert(AbstractConstantExpression.this);
            }
        };
    }
}
