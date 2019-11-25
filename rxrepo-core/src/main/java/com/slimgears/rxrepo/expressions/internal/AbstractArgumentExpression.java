package com.slimgears.rxrepo.expressions.internal;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ArgumentExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

@SuppressWarnings("UnstableApiUsage")
public abstract class AbstractArgumentExpression<S, T> extends AbstractObjectExpression<S, T> implements ArgumentExpression<S, T> {
    @Override
    protected Reflect<S, T> createReflect() {
        return new AbstractReflect<S, T>(this) {
            @Override
            public TypeToken<T> objectType() {
                return AbstractArgumentExpression.this.argType();
            }

            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitArgument(AbstractArgumentExpression.this);
            }

            @Override
            public ObjectExpression<S, T> convert(Converter converter) {
                return converter.convert(AbstractArgumentExpression.this);
            }
        };
    }
}
