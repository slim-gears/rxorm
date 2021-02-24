package com.slimgears.rxrepo.orientdb;

import com.google.common.reflect.TypeToken;
import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.rxrepo.util.PropertyResolver;

@SuppressWarnings("UnstableApiUsage")
class OElementPropertyResolver extends AbstractOrientPropertyResolver {
    private final OElement oElement;

    private OElementPropertyResolver(OrientDbSessionProvider dbSessionProvider, OElement oElement) {
        super(dbSessionProvider);
        this.oElement = oElement;
    }

    @Override
    public Iterable<String> propertyNames() {
        return oElement.getPropertyNames();
    }

    @Override
    protected Object getPropertyInternal(String name, TypeToken<?> type) {
        return oElement.getProperty(name);
    }

    static PropertyResolver create(OrientDbSessionProvider dbSessionProvider, OElement oElement) {
        return new OElementPropertyResolver(dbSessionProvider, oElement).cache();
    }
}
