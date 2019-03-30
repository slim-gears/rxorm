package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Lazy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.*;

public class OResultPropertyResolver extends AbstractOrientPropertyResolver {
    private final OResult oResult;
    private final String prefix;
    private final int index;
    private final Lazy<Iterable<String>> propertyNames;
    private final Map<String, PropertyResolver> resolvers = new HashMap<>();

    private OResultPropertyResolver(Supplier<ODatabaseDocument> dbSession, OResult oResult) {
        this(dbSession, oResult, "");
    }

    private OResultPropertyResolver(Supplier<ODatabaseDocument> dbSession, OResult oResult, String prefix) {
        super(dbSession);
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
    public Object getKey(Class keyClass) {
        return oResult.getIdentity().map(Object::toString).orElse(null);
    }

    @Override
    protected Object getPropertyInternal(String name, Class type) {
        return Optional
                .<Object>ofNullable(resolvers.get(name))
                .orElseGet(() -> oResult.getProperty(prefix + name));
    }

    public static PropertyResolver create(Supplier<ODatabaseDocument> dbSession, OResult oResult) {
        return Optional.ofNullable(oResult)
                .map(or -> new OResultPropertyResolver(dbSession, or))
                .orElse(null);
    }

    private Iterable<String> retrievePropertyNames() {
        Map<String, List<String>> map = oResult.getPropertyNames()
                .stream()
                .filter(n -> n.startsWith(prefix))
                .collect(groupingBy(
                        name -> split(name)[index],
                        mapping(name -> name.substring(prefix.length()), toList())));

        map.entrySet()
                .stream()
                .filter(e -> !e.getValue().isEmpty() && e.getValue().get(0).length() > e.getKey().length())
                .forEach(e -> resolvers.put(e.getKey(), new OResultPropertyResolver(dbSession, oResult, e.getKey() + ".")));

        return map.keySet();
    }

    private static String[] split(String name) {
        String[] parts = name.split("\\.");
        return parts.length > 0 ? parts : new String[] {name};
    }
}
