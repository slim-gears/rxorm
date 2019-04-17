package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.rxrepo.util.PropertyResolver;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractOrientPropertyResolver implements PropertyResolver {
    final OrientDbSessionProvider dbSessionProvider;

    AbstractOrientPropertyResolver(OrientDbSessionProvider dbSessionProvider) {
        this.dbSessionProvider = dbSessionProvider;
    }

    @Override
    public Object getProperty(String name, Class type) {
        Object obj = getPropertyInternal(name, type);
        return toValue(obj, type);
    }

    private Object toValue(Object obj, Class expectedType) {
        return toValue(dbSessionProvider, obj, expectedType);
    }

    @SuppressWarnings("unchecked")
    private static Object toValue(OrientDbSessionProvider dbSessionProvider, Object obj, Class expectedType) {
        if (obj instanceof OElement) {
            return OElementPropertyResolver.create(dbSessionProvider, (OElement)obj);
        } else if (expectedType.isEnum() && obj != null) {
            return Enum.valueOf(expectedType, obj.toString());
        } else if (obj instanceof ORecordId) {
            return toValue(dbSessionProvider, dbSessionProvider.withSession((ODatabaseDocument s) -> s.load((ORecordId)obj)), expectedType);
        } else if (obj instanceof OTrackedList) {
            return ((OTrackedList<OElement>)obj)
                    .stream()
                    .map(v -> toValue(dbSessionProvider, v, expectedType))
                    .collect(Collectors.toList());
        } else if (obj instanceof OTrackedSet) {
            return ((OTrackedSet<OElement>)obj)
                    .stream()
                    .map(v -> toValue(dbSessionProvider, v, expectedType))
                    .collect(Collectors.toSet());
        } else if (obj instanceof OTrackedMap) {
            return ((OTrackedMap<OElement>)obj)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> toValue(dbSessionProvider, e.getValue(), expectedType)));
        } else if (obj instanceof OResult) {
            return OResultPropertyResolver.create(dbSessionProvider, (OResult)obj);
        } else if (obj instanceof OResultSet) {
            return ((OResultSet)obj)
                    .stream()
                    .map(r -> toValue(dbSessionProvider, r, expectedType))
                    .collect(Collectors.toList());
        }
        return obj;
    }

    protected abstract Object getPropertyInternal(String name, Class type);
}
