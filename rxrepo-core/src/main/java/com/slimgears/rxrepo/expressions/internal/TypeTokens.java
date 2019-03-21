package com.slimgears.rxrepo.expressions.internal;

import com.slimgears.util.reflect.TypeToken;

import java.util.Collection;

public class TypeTokens {
    public static <T> TypeToken<T> element(TypeToken<? extends Collection<T>> collectionToken) {
        TypeToken<?>[] args = collectionToken.typeArguments();
        if (args.length != 1) {
            throw new RuntimeException("Cannot determine element type of " + collectionToken);
        }
        //noinspection unchecked
        return (TypeToken<T>)args[0];
    }

    public static <T> TypeToken<Collection<T>> collection(TypeToken<? extends T> element) {
        return TypeToken.ofParameterized(Collection.class, element);
    }
}
