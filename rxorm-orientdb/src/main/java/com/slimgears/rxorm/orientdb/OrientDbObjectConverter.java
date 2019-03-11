package com.slimgears.rxorm.orientdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.inject.Provider;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.slimgears.rxorm.orientdb.OrientDbQueryProvider.toOType;

public class OrientDbObjectConverter implements ObjectConverter {
    private final Provider<ODatabaseSession> sessionProvider;

    public OrientDbObjectConverter(Provider<ODatabaseSession> sessionProvider) {
        this.sessionProvider = sessionProvider;
    }

    @Override
    public <T> T toObject(OElement element, TypeToken<? extends T> type, List<PropertyMeta<T, ?, ?>> properties) {
        return toObject(type, element);
    }

    @Override
    public <T> T toObject(OElement element, MetaClass<T, ?> metaClass, List<PropertyMeta<T, ?, ?>> properties) {
        return toObject(metaClass, element);
    }

    @Override
    public <T> OElement toElement(T object) {
        return null;
    }

    private <T> T toObject(TypeToken<? extends T> type, OElement element) {
        //noinspection unchecked
        MetaClass<T, ? extends BuilderPrototype<T, ?>> metaClass = MetaClasses.forToken((TypeToken)type);
        return toObject(metaClass, element);
    }

    private <T> T toObject(MetaClass<T, ?> metaClass, OElement element) {
        if (element == null) {
            return null;
        }

        BuilderPrototype<T, ?> builder = metaClass.createBuilder();
        Streams.fromIterable(metaClass.properties())
                .forEach((PropertyMeta p) -> {
                    OType otype = toOType(p.type());
                    Object val = element.getProperty(p.name());
                    if (val == null) {
                        p.setValue(builder, null);
                        return;
                    }

                    if (!otype.isLink() && !otype.isEmbedded()) {
                        if (p.type().asClass().isEnum()) {
                            //noinspection unchecked
                            val = Enum.valueOf(p.type().asClass(), val.toString());
                        }
                        //noinspection unchecked
                        p.setValue(builder, val);
                    } else {
                        if (otype.isMultiValue()) {
                            if (val instanceof OTrackedList) {
                                //noinspection unchecked
                                p.setValue(builder, ((OTrackedList)val)
                                        .stream()
                                        .map(el -> toObject(typeParam(p.type()), (OElement)el))
                                        .collect(ImmutableList.toImmutableList()));
                            } else if (val instanceof OTrackedMap) {
                                //noinspection unchecked
                                p.setValue(builder, ((OTrackedMap)val)
                                        .entrySet()
                                        .stream()
                                        .collect(ImmutableMap.<Map.Entry<String, Object>, String, Object>toImmutableMap(
                                                Map.Entry::getKey,
                                                entry -> toObject(typeParam(p.type()), (OElement)entry.getValue()))));
                            } else if (val instanceof OTrackedSet) {
                                //noinspection unchecked
                                p.setValue(builder, ((OTrackedSet)val)
                                        .stream()
                                        .map(el -> toObject(typeParam(p.type()), (OElement)el))
                                        .collect(ImmutableSet.toImmutableSet()));
                            } else if (val instanceof List){
                                //noinspection unchecked
                                p.setValue(builder, ImmutableList.copyOf((List)val));
                            } else if (val instanceof Set) {
                                p.setValue(builder, ImmutableSet.copyOf((Set)val));
                            } else if (val instanceof Map) {
                                p.setValue(builder, ImmutableMap.copyOf((Map)val));
                            } else {
                                throw new RuntimeException("Not supported value type: " + val.getClass().getName());
                            }
                        } else {
                            if (val instanceof OElement) {
                                p.setValue(builder, toObject(p.type(), (OElement)val));
                            } else if (val instanceof ORecordId) {
                                p.setValue(builder, toObject(p.type(), sessionProvider.get().getRecord((ORecordId)val)));
                            } else {
                                throw new RuntimeException("Not supported value type: " + val.getClass().getName());
                            }
                        }
                    }
                });
        return builder.build();
    }

    private static <T, R> TypeToken<R> typeParam(TypeToken<T> token) {
        //noinspection unchecked
        return Optional
                .of(token.type())
                .flatMap(Optionals.ofType(ParameterizedType.class))
                .map(ParameterizedType::getActualTypeArguments)
                .filter(t -> t.length == 1)
                .map(t -> t[0])
                .flatMap(Optionals.ofType(Class.class))
                .map(c -> (Class<R>)c)
                .map(TypeToken::of)
                .orElse(null);
    }


}
