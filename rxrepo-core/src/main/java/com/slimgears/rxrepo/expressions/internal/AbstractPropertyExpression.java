package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.util.stream.Lazy;

import java.util.Objects;

public abstract class AbstractPropertyExpression<S, T, V>
    extends AbstractObjectExpression<S, V>
    implements PropertyExpression<S, T, V> {

    private final Lazy<String> lazyPath = Lazy.of(() -> PropertyExpressions.pathOf(this));
    private final Lazy<Integer> lazyHash = Lazy.of(() -> Objects.hash(target(), property()));

    @Override
    public String path() {
        return lazyPath.get();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public final boolean equals(Object obj) {
        return PropertyExpressions.propertyEquals(this, obj);
    }

    @Override
    public final int hashCode() {
        return lazyHash.get();
    }

    @Override
    protected Reflect<S, V> createReflect() {
        return new AbstractReflect<S, V>(this) {
            @Override
            public <_T> _T accept(Visitor<_T> visitor) {
                return visitor.visitProperty(AbstractPropertyExpression.this);
            }

            @Override
            public ObjectExpression<S, V> convert(Converter converter) {
                ObjectExpression<S, T> target = target().reflect().convert(converter);
                return converter.convert(target == target()
                    ? AbstractPropertyExpression.this
                    : ObjectPropertyExpression.create(type(), target, property()));
            }
        };
    }
}
