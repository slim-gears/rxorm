package com.slimgears.rxrepo.expressions.internal;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.CollectionOperationExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Collection;

@SuppressWarnings("UnstableApiUsage")
public abstract class AbstractCollectionOperationReflect<S, T, M, R, CT extends Collection<T>, CR extends Collection<R>>
    extends AbstractReflect<S, CR> {
    private final CollectionOperationExpression<S, T, M, R, CT, CR> expression;

    AbstractCollectionOperationReflect(CollectionOperationExpression<S, T, M, R, CT, CR> expression) {
        super(expression);
        this.expression = expression;
    }

    @Override
    public abstract TypeToken<CR> objectType();

    @Override
    public <_T> _T accept(ObjectExpression.Visitor<_T> visitor) {
        return visitor.visitOther(expression());
    }

    @Override
    public ObjectExpression<S, CR> convert(ObjectExpression.Converter converter) {
        ObjectExpression<S, CT> source = expression.source().reflect().convert(converter);
        ObjectExpression<T, M> operation = expression.operation().reflect().convert(converter);
        return converter.convert(source == expression.source() && operation == expression.operation()
            ? expression
            : create(source, operation));
    }

    protected abstract ObjectExpression<S, CR> create(ObjectExpression<S, CT> source, ObjectExpression<T, M> operation);
}
