package com.slimgears.rxrepo.expressions.internal;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ComposedExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.generic.MoreStrings;

public abstract class AbstractComposedExpression<S, T, R>
    extends AbstractObjectExpression<S, R>
    implements ComposedExpression<S, T, R> {

    @Override
    protected Reflect<S, R> createReflect() {
        return new AbstractReflect<S, R>(this) {
            @SuppressWarnings("UnstableApiUsage")
            @Override
            public TypeToken<R> objectType() {
                return AbstractComposedExpression.this.expression().reflect().objectType();
            }

            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitComposed(AbstractComposedExpression.this);
            }

            @Override
            public ObjectExpression<S, R> convert(Converter converter) {
                ObjectExpression<S, T> source = AbstractComposedExpression.this.source().reflect().convert(converter);
                ObjectExpression<T, R> expression = AbstractComposedExpression.this.expression().reflect().convert(converter);
                return converter.convert (
                    source == source() && expression == AbstractComposedExpression.this.expression()
                    ? AbstractComposedExpression.this
                    : ObjectComposedExpression.create(type(), source, expression));
            }
        };
    }

    @Override
    public String toString() {
        return MoreStrings.format("{}({}, {})", type().name(), source(), expression());
    }
}
