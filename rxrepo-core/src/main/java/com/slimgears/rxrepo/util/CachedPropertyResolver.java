package com.slimgears.rxrepo.util;

import com.slimgears.util.stream.Lazy;

import java.util.concurrent.ConcurrentHashMap;

public class CachedPropertyResolver implements PropertyResolver {
    private final PropertyResolver underlyingResolver;
    private final Lazy<Iterable<String>> propertyNames;
    private final Lazy<ConcurrentHashMap<String, Object>> values;

    private CachedPropertyResolver(PropertyResolver underlyingResolver) {
        this.underlyingResolver = underlyingResolver;
        this.propertyNames = Lazy.of(underlyingResolver::propertyNames);
        this.values = Lazy.of(ConcurrentHashMap::new);
    }

    public static PropertyResolver of(PropertyResolver underlyingResolver) {
        return new CachedPropertyResolver(underlyingResolver);
    }

    @Override
    public Iterable<String> propertyNames() {
        return propertyNames.get();
    }

    @Override
    public Object getProperty(String name, Class<?> type) {
        return values.get().computeIfAbsent(name, n -> underlyingResolver.getProperty(n, type));
    }
}
