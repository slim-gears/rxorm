package com.slimgears.rxrepo.mongodb.codecs;

import com.google.common.base.Strings;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Streams;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.slimgears.util.stream.Optionals.ofType;

public class MetaClassCodec<T extends HasMetaClass<T>> implements Codec<T> {
    private final MetaClass<T> metaClass;
    private final CodecRegistry codecRegistry;
    private final Lazy<Optional<Function<T, String>>> textSupplier;

    private MetaClassCodec(Class<T> clazz, CodecRegistry codecRegistry) {
        this.metaClass = MetaClasses.forClass(clazz);
        this.textSupplier = Lazy.of(this::searchableTextFromEntity);
        this.codecRegistry = codecRegistry;
    }

    static <T extends HasMetaClass<T>> Codec<T> create(Class<T> clazz, CodecRegistry codecRegistry) {
        return new MetaClassCodec<>(clazz, codecRegistry);
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();
        MetaBuilder<T> builder = metaClass.createBuilder();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            PropertyMeta<T, ?> propertyMeta = fromFieldName(name);
            if (propertyMeta != null) {
                readProperty(reader, propertyMeta, builder, decoderContext);
            } else {
                reader.skipValue();
            }
        }
        reader.readEndDocument();
        return builder.build();
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        writer.writeStartDocument();
        metaClass.properties()
                .forEach(p -> writeProperty(writer, p, value, encoderContext));
        textSupplier.get()
                .ifPresent(func -> {
                    String text = func.apply(value);
                    writer.writeName("_text");
                    writer.writeString(text);
                });
        writer.writeEndDocument();
    }

    @Override
    public Class<T> getEncoderClass() {
        return metaClass.asClass();
    }

    private PropertyMeta<T, ?> fromFieldName(String fieldName) {
        return "_id".equals(fieldName)
                ? Optional.of(metaClass)
                .map(mc -> mc instanceof MetaClassWithKey
                        ? (MetaClassWithKey<?, T>)mc
                        : null)
                .map(HasKeyProperty::keyProperty)
                .orElse(null)
                : metaClass.getProperty(fieldName);
    }

    private String toFieldName(PropertyMeta<T, ?> property) {
        return isKeyProperty(property) ? "_id" : property.name();
    }

    private boolean isKeyProperty(PropertyMeta<T, ?> property) {
        return Optional
                .ofNullable(property.declaringType())
                .flatMap(ofType(MetaClassWithKey.class))
                .map(mc -> mc.keyProperty() == property)
                .orElse(false);
    }

    private <V> void writeProperty(BsonWriter writer, PropertyMeta<T, V> propertyMeta, T object, EncoderContext context) {
        V val = propertyMeta.getValue(object);
        if (val != null) {
            writer.writeName(toFieldName(propertyMeta));
            if (PropertyMetas.isReference(propertyMeta)) {
                MetaClassWithKey<?, V> metaClass = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
                writeKey(writer, metaClass, val, context);
            } else {
                writeValue(writer, propertyMeta.type().asClass(), val, context);
            }
        }
    }

    private <K, V> void writeKey(BsonWriter writer, MetaClassWithKey<K, V> metaClassWithKey, V value, EncoderContext context) {
        writeValue(writer, metaClassWithKey.keyProperty().type().asClass(), metaClassWithKey.keyOf(value), context);
    }

    private <V> void writeValue(BsonWriter writer, Class<V> valueClass, V value, EncoderContext context) {
        Codec<V> codec = codecRegistry.get(valueClass);
        codec.encode(writer, value, context);
    }

    private <V> void readProperty(BsonReader reader, PropertyMeta<T, V> propertyMeta, MetaBuilder<T> builder, DecoderContext context) {
        if (reader.getCurrentBsonType() == BsonType.NULL) {
            reader.readNull();
        } else {
            Codec<V> codec = codecRegistry.get(propertyMeta.type().asClass());
            Optional.ofNullable(codec.decode(reader, context))
                    .ifPresent(val -> propertyMeta.setValue(builder, val));
        }
    }

    private Optional<Function<T, String>> searchableTextFromEntity() {
        return searchableTextFromEntity(metaClass, e -> e, new HashSet<>());
    }

    private <R> Optional<Function<T, String>> searchableTextFromEntity(MetaClass<R> metaClass, Function<T, R> getter, Set<PropertyMeta<?, ?>> visitedProperties) {
        Optional<Function<T, String>> selfFields = searchableTextForMetaClass(metaClass, visitedProperties).map(getter::andThen);
        Optional<Function<T, String>> nestedFields = Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.type().is(HasMetaClass.class::isAssignableFrom))
                .filter(visitedProperties::add)
                .map(p -> searchableTextFromProperty(getter, p, visitedProperties))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .reduce(MetaClassCodec::combine);

        if (selfFields.isPresent() && nestedFields.isPresent()) {
            return Optional.of(combine(selfFields.get(), nestedFields.get()));
        } else if (selfFields.isPresent()) {
            return selfFields;
        } else {
            return nestedFields;
        }
    }

    private <R, V> Optional<Function<T, String>> searchableTextFromProperty(Function<T, R> getter, PropertyMeta<R, V> propertyMeta, Set<PropertyMeta<?, ?>> visitedProperties) {
        Function<T, V> nextGetter = val -> Optional.ofNullable(getter.apply(val)).map(propertyMeta::getValue).orElse(null);
        MetaClass<V> metaClass = MetaClasses.forTokenUnchecked(propertyMeta.type());
        return searchableTextFromEntity(metaClass, nextGetter, visitedProperties);
    }

    private static <T> Function<T, String> combine(Function<T, String> first, Function<T, String> second) {
        return entity -> combineStrings(first.apply(entity), second.apply(entity));
    }

    private static <T> Optional<Function<T, String>> searchableTextForMetaClass(MetaClass<T> metaClass, Set<PropertyMeta<?, ?>> visitedProperties) {
        return Streams
                .fromIterable(metaClass.properties())
                .filter(p -> p.hasAnnotation(Searchable.class))
                .filter(visitedProperties::add)
                .<Function<T, String>>map(p -> (entity -> Optional
                        .ofNullable(entity)
                        .map(p::getValue)
                        .map(Object::toString)
                        .orElse("")))
                .reduce(MetaClassCodec::combine);
    }

    private static String combineStrings(String first, String second) {
        if (Strings.isNullOrEmpty(first)) {
            return second != null ? second : "";
        }
        if (Strings.isNullOrEmpty(second)) {
            return first;
        }
        return first + " " + second;
    }
}
