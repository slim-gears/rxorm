package com.slimgears.rxrepo.util;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.annotations.Embedded;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"WeakerAccess", "UnstableApiUsage"})
public class PropertyMetas {
    private final static Map<PropertyMeta<?, ?>, Boolean> referencePropertiesCache = new ConcurrentHashMap<>();
    private final static Map<PropertyMeta<?, ?>, Boolean> referenceCollectionPropertiesCache = new ConcurrentHashMap<>();
    private final static Map<PropertyMeta<?, ?>, Boolean> embeddedPropertiesCache = new ConcurrentHashMap<>();
    private final static Map<PropertyMeta<?, ?>, Boolean> mandatoryPropertiesCache = new ConcurrentHashMap<>();
    private final static Map<PropertyMeta<?, ?>, Boolean> keyPropertiesCache = new ConcurrentHashMap<>();

    public static boolean isReference(PropertyMeta<?, ?> propertyMeta) {
        return referencePropertiesCache.computeIfAbsent(propertyMeta, pm -> isReference(pm.type()) && !pm.hasAnnotation(Embedded.class));
    }

    public static boolean isReferenceCollection(PropertyMeta<?, ?> propertyMeta) {
        return referenceCollectionPropertiesCache.computeIfAbsent(propertyMeta, pm -> isReferenceCollection(pm.type()) && !pm.hasAnnotation(Embedded.class));
    }

    public static boolean isEmbedded(PropertyMeta<?, ?> propertyMeta) {
        return embeddedPropertiesCache.computeIfAbsent(propertyMeta, pm -> isEmbedded(pm.type()) || (isReference(pm.type()) && pm.hasAnnotation(Embedded.class)));
    }

    public static boolean isReference(TypeToken<?> typeToken) {
        return typeToken.isSubtypeOf(HasMetaClassWithKey.class);
    }

    public static boolean isReferenceCollection(TypeToken<?> typeToken) {
        return typeToken.isSubtypeOf(Collection.class) && isReference(MoreTypeTokens.argType(typeToken, Collection.class));
    }

    public static Optional<TypeToken<?>> getReferencedType(PropertyMeta<?, ?> propertyMeta) {
        if (isReference(propertyMeta)) {
            return Optional.of(propertyMeta.type());
        } else if (isReferenceCollection(propertyMeta)) {
            return Optional.of(MoreTypeTokens.argType(propertyMeta.type(), Collection.class));
        }
        return Optional.empty();
    }

    public static boolean isEmbedded(TypeToken<?> typeToken) {
        return hasMetaClass(typeToken) && !isReference(typeToken);
    }

    public static boolean hasMetaClass(TypeToken<?> typeToken) {
        return typeToken.isSubtypeOf(HasMetaClass.class);
    }

    public static boolean hasMetaClass(PropertyMeta<?, ?> property) {
        return hasMetaClass(property.type());
    }

    public static boolean isKey(PropertyMeta<?, ?> property) {
        return keyPropertiesCache.computeIfAbsent(property, pm ->
            Optional.of(pm.declaringType())
                .flatMap(Optionals.ofType(MetaClassWithKey.class))
                .map(mc -> mc.keyProperty() == pm)
                .orElse(false));
    }

    public static boolean isMandatory(PropertyMeta<?, ?> propertyMeta) {
        return mandatoryPropertiesCache.computeIfAbsent(propertyMeta, pm -> !pm.hasAnnotation(Nullable.class));
    }
}
