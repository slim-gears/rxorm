package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Lazy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

class OResultPropertyResolver extends AbstractOrientPropertyResolver {
    private final OResult oResult;
    private final String prefix;
    private final int index;
    private final Lazy<Iterable<String>> propertyNames;
    private final Map<String, Lazy<PropertyResolver>> resolvers = new HashMap<>();
    private final BiMap<String, String> propertyToCanonicNameMap = HashBiMap.create();
    private final BiMap<String, String> propertyFromCanonicNameMap = propertyToCanonicNameMap.inverse();

    private OResultPropertyResolver(OrientDbSessionProvider dbSessionProvider, OResult oResult) {
        this(dbSessionProvider, oResult, "");
    }

    private OResultPropertyResolver(OrientDbSessionProvider dbSessionProvider, OResult oResult, String prefix) {
        super(dbSessionProvider);
        this.oResult = oResult;
        this.prefix = prefix;
        this.index = (int)Arrays.stream(split(prefix)).filter(p -> !p.isEmpty()).count();
        this.propertyNames = Lazy.of(this::retrievePropertyNames);
    }

    @Override
    public Iterable<String> propertyNames() {
        return propertyNames.get();
    }

    @Override
    protected Object getPropertyInternal(String name, Class type) {
        propertyNames.get();
        return Optional
                .ofNullable(resolvers.get(fromCanonic(name)))
                .map(Lazy::get)
                .map(Object.class::cast)
                .orElseGet(() -> {
                    Object obj = oResult.getProperty(prefix + fromCanonic(name));
                    if (obj instanceof OResult) {
                        PropertyResolver resolver = OResultPropertyResolver.create(dbSessionProvider, (OResult)obj);
                        resolvers.put(prefix + fromCanonic(name), Lazy.of(() -> resolver));
                        obj = resolver;
                    }
                    return obj;
                });
    }

    static PropertyResolver create(OrientDbSessionProvider dbSessionProvider, OResult oResult) {
        return Optional.ofNullable(oResult)
                .map(or -> new OResultPropertyResolver(dbSessionProvider, or).cache())
                .orElse(null);
    }

    private Collection<String> getResultPropertyNames() {
        return oResult.isElement()
                ? Sets.union(oResult.getPropertyNames(), Collections.singleton("@version"))
                : oResult.getPropertyNames();
    }

    private Iterable<String> retrievePropertyNames() {
        Map<String, List<String>> map = getResultPropertyNames()
                .stream()
                .filter(n -> n.startsWith(prefix))
                .filter(n -> oResult.getProperty(n) != null)
                .peek(this::toCanonic)
                .collect(groupingBy(
                        name -> split(name)[index],
                        mapping(name -> name.substring(prefix.length()), toList())));

        map.entrySet()
                .stream()
                .filter(e -> !e.getValue().isEmpty() && e.getValue().get(0).length() > e.getKey().length())
                .forEach(e -> resolvers.put(e.getKey(), Lazy.of(() -> new OResultPropertyResolver(dbSessionProvider, oResult, prefix + e.getKey() + "."))));

        return map.keySet().stream().map(this::toCanonic).collect(Collectors.toSet());
    }

    private String toCanonic(String name) {
        return propertyToCanonicNameMap.computeIfAbsent(name, n -> n.replace("`", ""));
    }

    private String fromCanonic(String name) {
        return propertyFromCanonicNameMap.getOrDefault(name, name);
    }

    private String[] split(String name) {
        String[] parts = name.split("\\.");
        return parts.length > 0 ? parts : new String[] {name};
    }
}
