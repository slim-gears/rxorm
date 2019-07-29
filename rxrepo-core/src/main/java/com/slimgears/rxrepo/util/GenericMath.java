package com.slimgears.rxrepo.util;

import java.math.BigDecimal;
import java.math.BigInteger;

@SuppressWarnings({"unchecked", "WeakerAccess"})
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
            return (T)((BigDecimal)left).divide((BigDecimal) right, BigDecimal.ROUND_HALF_UP);
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

    public static <T> T fromNumber(Number number, Class<T> cls) {
        if (cls.isInstance(number)) {
            return (T)number;
        }
        if (cls == Long.class || cls == long.class) {
            return (T)(Number)number.longValue();
        } else if (cls == Integer.class || cls == int.class) {
            return (T)(Number)number.intValue();
        } else if (cls == Short.class || cls == short.class) {
            return (T)(Number)number.shortValue();
        } else if (cls == Float.class || cls == float.class) {
            return (T)(Number)number.floatValue();
        } else if (cls == Double.class || cls == double.class) {
            return (T)(Number)number.doubleValue();
        } else if (cls == Byte.class || cls == byte.class) {
            return (T)(Number)number.byteValue();
        } else {
            return null;
        }
    }
}
