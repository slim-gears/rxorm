package com.slimgears.rxrepo.expressions.internal;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.slimgears.util.reflect.TypeTokens;

import java.util.Collection;
import java.util.Map;

@SuppressWarnings("UnstableApiUsage")
public class MoreTypeTokens {
    public static <T, I extends Iterable<T>> TypeToken<T> elementType(TypeToken<I> iterableToken) {
        return argType(iterableToken, Iterable.class);
    }

    public static <K, M extends Map<K, ?>> TypeToken<K> keyType(TypeToken<M> mapToken) {
        return argType(mapToken, Map.class, 0);
    }

    public static <V, M extends Map<?, V>> TypeToken<V> valueType(TypeToken<M> mapToken) {
        return argType(mapToken, Map.class, 1);
    }

    public static <T> TypeToken<T> argType(TypeToken<?> type, Class<?> rawClass) {
        return argType(type, rawClass, 0);
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeToken<T> argType(TypeToken<?> type, Class<?> rawClass, int argNum) {
        return (TypeToken<T>)type.resolveType(rawClass.getTypeParameters()[argNum]);
    }

    public static <T> TypeToken<Collection<T>> collection(TypeToken<T> element) {
        return new TypeToken<Collection<T>>() {}
        .where(new TypeParameter<T>() {}, element);
    }

    public static boolean isEnum(TypeToken<?> typeToken) {
        return typeToken.getRawType().isEnum();
    }

    public static boolean hasNoTypeVars(TypeToken<?> typeToken) {
        return !TypeTokens.hasTypeVars(typeToken);
    }
}
