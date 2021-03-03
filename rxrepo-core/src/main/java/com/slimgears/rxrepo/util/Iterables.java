package com.slimgears.rxrepo.util;

import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class Iterables {
    @SuppressWarnings("unchecked")
    public static <T> Collection<T> toCollection(@Nonnull Iterable<T> items) {
        return Optional.of(items)
                .flatMap(Optionals.ofType(Collection.class))
                .map(c -> (Collection<T>)c)
                .orElseGet(() -> Streams.fromIterable(items).collect(Collectors.toList()));
    }
}
