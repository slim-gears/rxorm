package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.sql.SchemaProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class OrientDbSchemaProvider implements SchemaProvider {
    private final Supplier<ODatabaseDocument> dbSession;
    private final Map<String, OClass> classMap = new ConcurrentHashMap<>();

    public OrientDbSchemaProvider(Supplier<ODatabaseDocument> dbSession) {
        this.dbSession = dbSession;
    }

    @Override
    public <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass) {
        return Completable.fromAction(() -> ensureClass(metaClass));
    }

    @Override
    public <K, T> String tableName(MetaClassWithKey<K, T> metaClass) {
        return metaClass.objectClass().asClass().getSimpleName();
    }

    private <T> OClass ensureClass(MetaClass<T> metaClass) {
        return classMap.computeIfAbsent(toClassName(metaClass.objectClass()), name -> createClass(metaClass));
    }

    @SuppressWarnings("unchecked")
    private <T> OClass createClass(MetaClass<T> metaClass) {
        String className = toClassName(metaClass.objectClass());
        OClass oClass = dbSession.get().createClassIfNotExist(className);
        Streams.fromIterable(metaClass.properties())
                .forEach(p -> addProperty(oClass, p));

        if (metaClass instanceof MetaClassWithKey) {
            MetaClassWithKey metaClassWithKey = (MetaClassWithKey) metaClass;
            addIndex(oClass, metaClassWithKey.keyProperty(), true);
        }

        Streams.fromIterable(metaClass.properties())
                .filter(p -> p.hasAnnotation(Indexable.class) && !p.hasAnnotation(Key.class))
                .forEach(p -> addIndex(oClass, p, p.getAnnotation(Indexable.class).unique()));

        Streams.fromIterable(metaClass.properties())
                .filter(p -> p.type().is(HasMetaClassWithKey.class::isAssignableFrom))
                .map(PropertyMeta::type)
                .map(OrientDbSchemaProvider::toMetaClass)
                .forEach(this::ensureClass);

        String[] textFields = Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.hasAnnotation(Searchable.class))
                .map(PropertyMeta::name)
                .toArray(String[]::new);

        if (textFields.length > 0) {
            oClass.createIndex(className + ".textIndex", "FULLTEXT", null, null, "LUCENE", textFields);
        }

        return oClass;
    }

    private static <T> void addIndex(OClass oClass, PropertyMeta<T, ?> propertyMeta, boolean unique) {
        String className = toClassName(propertyMeta.declaringType().objectClass());
        OClass.INDEX_TYPE indexType = unique ? OClass.INDEX_TYPE.UNIQUE : OClass.INDEX_TYPE.NOTUNIQUE;
        if (!oClass.areIndexed(propertyMeta.name())) {
            oClass.createIndex(className + "." + propertyMeta.name() + "Index", indexType, propertyMeta.name());
        }
    }

    private static <T extends HasMetaClass<T>> MetaClass<T> toMetaClass(TypeToken typeToken) {
        //noinspection unchecked
        return MetaClasses.forToken((TypeToken<T>)typeToken);
    }

    private <T> void addProperty(OClass oClass, PropertyMeta<T, ?> propertyMeta) {
        OType propertyOType = toOType(propertyMeta.type());
        if (propertyOType.isLink() || propertyOType.isEmbedded()) {
            OClass linkedOClass = dbSession.get().getClass(toClassName(propertyMeta.type()));
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
                if (!Objects.equals(oProperty.getLinkedClass(), linkedOClass)) {
                    oProperty.setLinkedClass(linkedOClass);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType, linkedOClass);
            }
        } else {
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType);
            }
        }
    }

    static OType toOType(TypeToken<?> token) {
        Class<?> cls = token.asClass();
        return Optional
                .ofNullable(OType.getTypeByClass(cls))
                .orElseGet(() -> HasMetaClass.class.isAssignableFrom(cls)
                        ? (HasMetaClassWithKey.class.isAssignableFrom(cls) ? OType.LINK : OType.EMBEDDED)
                        : OType.ANY);
    }

    static String toClassName(TypeToken<?> cls) {
        return toClassName(cls.asClass());
    }

    static String toClassName(Class<?> cls) {
        return cls.getSimpleName();
    }

}
