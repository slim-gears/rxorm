package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.rxrepo.util.PropertyResolver;

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
    protected Object getPropertyInternal(String name, Class type) {
        return oElement.getProperty(name);
    }

    static PropertyResolver create(OrientDbSessionProvider dbSessionProvider, OElement oElement) {
        return new OElementPropertyResolver(dbSessionProvider, oElement).cache();
    }
}
