package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.rxrepo.util.PropertyResolver;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractOrientPropertyResolver implements PropertyResolver {
    protected final Supplier<ODatabaseDocument> dbSession;

    protected AbstractOrientPropertyResolver(Supplier<ODatabaseDocument> dbSession) {
        this.dbSession = dbSession;
    }

    @Override
    public Object getProperty(String name, Class type) {
        Object obj = getPropertyInternal(name, type);
        return toValue(obj, type);
    }

    @SuppressWarnings("unchecked")
    protected Object toValue(Object obj, Class expectedType) {
        return toValue(dbSession, obj, expectedType);
    }

    static Object toValue(Supplier<ODatabaseDocument> dbSession, Object obj, Class expectedType) {
        if (obj instanceof OElement) {
            return OElementPropertyResolver.create(dbSession, (OElement)obj);
        } else if (expectedType.isEnum() && obj != null) {
            return Enum.valueOf(expectedType, obj.toString());
        } else if (obj instanceof ORecordId) {
            return toValue(dbSession, dbSession.get().load((ORecordId)obj), expectedType);
        } else if (obj instanceof OTrackedList) {
            return ((OTrackedList<OElement>)obj)
                    .stream()
                    .map(v -> toValue(dbSession, v, expectedType))
                    .collect(Collectors.toList());
        } else if (obj instanceof OTrackedSet) {
            return ((OTrackedSet<OElement>)obj)
                    .stream()
                    .map(v -> toValue(dbSession, v, expectedType))
                    .collect(Collectors.toSet());
        } else if (obj instanceof OTrackedMap) {
            return ((OTrackedMap<OElement>)obj)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> toValue(dbSession, e.getValue(), expectedType)));
        }
        return obj;
    }

    protected abstract Object getPropertyInternal(String name, Class type);
}
