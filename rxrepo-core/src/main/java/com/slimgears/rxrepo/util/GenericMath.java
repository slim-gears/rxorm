package com.slimgears.rxrepo.util;

import java.math.BigDecimal;
import java.math.BigInteger;

@SuppressWarnings("unchecked")
public class GenericMath {
    public static <T extends Number> T add(T left, T right) {
        if (left instanceof Integer) {
            return (T)Integer.valueOf(left.intValue() + right.intValue());
        } else if (left instanceof Short) {
            return (T)Short.valueOf((short)(left.shortValue() + right.shortValue()));
        } else if (left instanceof Long) {
            return (T)Long.valueOf(left.longValue() + right.longValue());
        } else if (left instanceof Float) {
            return (T)Float.valueOf(left.floatValue() + right.floatValue());
        } else if (left instanceof Double) {
            return (T)Double.valueOf(left.doubleValue() + right.doubleValue());
        } else if (left instanceof BigInteger) {
            return (T)((BigInteger)left).add((BigInteger)right);
        } else if (left instanceof BigDecimal) {
            return (T)((BigDecimal)left).add((BigDecimal) right);
        }
        throw new IllegalArgumentException("Not supported operation for: " + left);
    }

    public static <T extends Number> T subtract(T left, T right) {
        if (left instanceof Integer) {
            return (T)Integer.valueOf(left.intValue() - right.intValue());
        } else if (left instanceof Short) {
            return (T)Short.valueOf((short)(left.shortValue() - right.shortValue()));
        } else if (left instanceof Long) {
            return (T)Long.valueOf(left.longValue() - right.longValue());
        } else if (left instanceof Float) {
            return (T)Float.valueOf(left.floatValue() - right.floatValue());
        } else if (left instanceof Double) {
            return (T)Double.valueOf(left.doubleValue() - right.doubleValue());
        } else if (left instanceof BigInteger) {
            return (T)((BigInteger)left).subtract((BigInteger)right);
        } else if (left instanceof BigDecimal) {
            return (T)((BigDecimal)left).subtract((BigDecimal) right);
        }
        throw new IllegalArgumentException("Not supported operation for: " + left);
    }

    public static <T extends Number> T multiply(T left, T right) {
        if (left instanceof Integer) {
            return (T)Integer.valueOf(left.intValue() * right.intValue());
        } else if (left instanceof Short) {
            return (T)Short.valueOf((short)(left.shortValue() * right.shortValue()));
        } else if (left instanceof Long) {
            return (T)Long.valueOf(left.longValue() * right.longValue());
        } else if (left instanceof Float) {
            return (T)Float.valueOf(left.floatValue() * right.floatValue());
        } else if (left instanceof Double) {
            return (T)Double.valueOf(left.doubleValue() * right.doubleValue());
        } else if (left instanceof BigInteger) {
            return (T)((BigInteger)left).multiply((BigInteger)right);
        } else if (left instanceof BigDecimal) {
            return (T)((BigDecimal)left).multiply((BigDecimal) right);
        }
        throw new IllegalArgumentException("Not supported operation for: " + left);
    }

    public static <T extends Number> T divide(T left, T right) {
        if (left instanceof Integer) {
            return (T)Integer.valueOf(left.intValue() / right.intValue());
        } else if (left instanceof Short) {
            return (T)Short.valueOf((short)(left.shortValue() / right.shortValue()));
        } else if (left instanceof Long) {
            return (T)Long.valueOf(left.longValue() / right.longValue());
        } else if (left instanceof Float) {
            return (T)Float.valueOf(left.floatValue() / right.floatValue());
        } else if (left instanceof Double) {
            return (T)Double.valueOf(left.doubleValue() / right.doubleValue());
        } else if (left instanceof BigInteger) {
            return (T)((BigInteger)left).divide((BigInteger)right);
        } else if (left instanceof BigDecimal) {
            return (T)((BigDecimal)left).divide((BigDecimal) right);
        }
        throw new IllegalArgumentException("Not supported operation for: " + left);
    }

    public static <T extends Number> T negate(T val) {
        if (val instanceof Integer) {
            return (T)Integer.valueOf(-val.intValue());
        } else if (val instanceof Short) {
            return (T)Short.valueOf((short)-val.shortValue());
        } else if (val instanceof Long) {
            return (T)Long.valueOf(-val.longValue());
        } else if (val instanceof Float) {
            return (T)Float.valueOf(-val.floatValue());
        } else if (val instanceof Double) {
            return (T)Double.valueOf(-val.doubleValue());
        } else if (val instanceof BigInteger) {
            return (T)((BigInteger)val).negate();
        } else if (val instanceof BigDecimal) {
            return (T)((BigDecimal)val).negate();
        }
        throw new IllegalArgumentException("Not supported operation for: " + val);
    }
}
