package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.rxrepo.util.PropertyResolver;

import java.util.function.Supplier;

public class OElementPropertyResolver extends AbstractOrientPropertyResolver {
    private final OElement oElement;

    private OElementPropertyResolver(Supplier<ODatabaseDocument> dbSession, OElement oElement) {
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

    public static PropertyResolver create(Supplier<ODatabaseDocument> dbSession, OElement oElement) {
        return new OElementPropertyResolver(dbSession, oElement);
    }
}
