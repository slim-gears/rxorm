package com.slimgears.rxorm.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.util.repository.util.PropertyResolver;

public class OElementPropertyResolver extends AbstractOrientPropertyResolver {
    private final OElement oElement;

    private OElementPropertyResolver(ODatabaseSession dbSession, OElement oElement) {
        super(dbSession);
        this.oElement = oElement;
    }

    @Override
    public Iterable<String> propertyNames() {
        return oElement.getPropertyNames();
    }

    @Override
    public Object getKey(Class keyClass) {
        return oElement.getIdentity().toString();
    }

    @Override
    protected Object getPropertyInternal(String name, Class type) {
        return oElement.getProperty(name);
    }

    public static PropertyResolver create(ODatabaseSession dbSession, OElement oElement) {
        return new OElementPropertyResolver(dbSession, oElement);
    }
}
