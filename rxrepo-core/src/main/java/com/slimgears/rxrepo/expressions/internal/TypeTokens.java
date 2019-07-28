package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public class TypeTokens {
    @SuppressWarnings("unchecked")
    public static <T, C extends Collection<T>> TypeToken<T> element(TypeToken<C> collectionToken) {
        TypeToken<?>[] args = collectionToken.typeArguments();
        if (args.length != 1) {
            throw new RuntimeException("Cannot determine element type of " + collectionToken);
        }
        return (TypeToken<T>)args[0];
    }

    public static <T> TypeToken<Collection<T>> collection(TypeToken<T> element) {
        return TypeToken.ofParameterized(Collection.class, element);
    }
}
