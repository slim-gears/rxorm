package com.slimgears.rxrepo.expressions;

import com.google.common.reflect.TypeToken;
import com.slimgears.util.autovalue.annotations.PropertyMeta;

@SuppressWarnings("WeakerAccess")
public abstract class ExpressionVisitor<_T, _R> {
    public _R visit(Expression expression, _T arg) {
        if (expression instanceof PropertyExpression) {
            return visitProperty((PropertyExpression<?, ?, ?>)expression, arg);
        } else if (expression instanceof UnaryOperationExpression) {
            return visitUnaryOperator((UnaryOperationExpression<?, ?, ?>)expression, arg);
        } else if (expression instanceof BinaryOperationExpression) {
            return visitBinaryOperator((BinaryOperationExpression<?, ?, ?, ?>)expression, arg);
        } else if (expression instanceof ConstantExpression) {
            return visitConstant((ConstantExpression<?, ?>)expression, arg);
        } else if (expression instanceof ComposedExpression) {
            return visitComposition((ComposedExpression<?, ?, ?>)expression, arg);
        } else if (expression instanceof ArgumentExpression) {
            return visitArgument((ArgumentExpression<?, ?>)expression, arg);
        } else if (expression instanceof DelegateExpression) {
            return visit(((DelegateExpression<?, ?>)expression).delegate(), arg);
        } else {
            return visitOther((ObjectExpression<?, ?>)expression, arg);
        }
    }

    protected abstract _R reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, _R first, _R second);
    protected abstract _R reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, _R first);

    protected <S, T> _R visitOther(ObjectExpression<S, T> expression, _T arg) {
        throw new IllegalArgumentException("Not supported expression type: " + expression.toString());
    }

    protected <S, T, R> _R visitComposition(ComposedExpression<S, T, R> expression, _T arg) {
        _R resSrc = this.visit(expression.source(), arg);
        _R resExp = this.visit(expression.expression(), arg);
        return reduceBinary(expression, expression.type(), resSrc, resExp);
    }

    protected <S, T> _R visitConstant(ConstantExpression<S, T> constantExpression, _T arg) {
        return visitConstant(constantExpression.type(), constantExpression.value(), arg);
    }

    protected <S, T, V> _R visitProperty(PropertyExpression<S, T, V> expression, _T arg) {
        return reduceBinary(expression, expression.type(), visit(expression.target(), arg), visitProperty(expression.property(), arg));
    }

    protected <S, T1, T2, R> _R visitBinaryOperator(BinaryOperationExpression<S, T1, T2, R> expression, _T arg) {
        return reduceBinary(expression, expression.type(), visit(expression.left(), arg), visit(expression.right(), arg));
    }

    protected <S, T, R> _R visitUnaryOperator(UnaryOperationExpression<S, T, R> expression, _T arg) {
        return reduceUnary(expression, expression.type(), visit(expression.operand(), arg));
    }

    protected <S, T> _R visitArgument(ArgumentExpression<S, T> expression, _T arg) {
        return visitArgument(expression.argType(), arg);
    }

    protected abstract <T, V> _R visitProperty(PropertyMeta<T, V> propertyMeta, _T arg);
    protected abstract <V> _R visitConstant(Expression.Type type, V value, _T arg);
    protected abstract <T> _R visitArgument(TypeToken<T> argType, _T arg);
}
