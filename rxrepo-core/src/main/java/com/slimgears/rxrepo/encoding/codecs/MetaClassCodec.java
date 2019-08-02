package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Lazy;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class MetaClassCodec<T> implements MetaCodec<T> {
    private final MetaClass<T> metaClass;
    private final Lazy<Optional<Function<T, String>>> textSupplier;
    private final MetaObjectResolver resolver;
    private final boolean alwaysEmbedNested;

    private MetaClassCodec(MetaClass<T> metaClass, MetaObjectResolver resolver) {
        this.metaClass = metaClass;
        this.resolver = resolver;
        this.textSupplier = Lazy.of(() -> MetaClassSearchableFields.searchableTextFromEntity(metaClass));
        this.alwaysEmbedNested = resolver == null;
    }

    public static <T> MetaCodec<T> forMetaClass(MetaClass<T> metaClass, MetaObjectResolver resolver) {
        return new MetaClassCodec<>(metaClass, resolver);
    }

    @Override
    public void encode(MetaContext.Writer context, T value) {
        context.writer().writeBeginObject();
        metaClass.properties()
                .forEach(p -> writeProperty(context, p, value));
        textSupplier.get()
                .ifPresent(func -> {
                    String text = func.apply(value);
                    context.writer().writeName(context.fieldMapper().searchableTextField());
                    context.writer().writeString(text);
                });
        context.writer().writeEndObject();
    }

    @Override
    public T decode(MetaContext.Reader context) {
        context.reader().readBeginObject();
        MetaBuilder<T> builder = metaClass.createBuilder();
        int foundProperties = 0;
        while (context.reader().currentElement() != MetaElementType.EndObject) {
            String name = context.reader().readName();
            Function<String, PropertyMeta<T, ?>> propertyGetter;
            Consumer<PropertyMeta<T, ?>> propertyReader;

            if (context.fieldMapper().isReferenceFieldName(name)) {
                propertyGetter = n -> context.fieldMapper().fromReferenceFieldName(metaClass, n);
                propertyReader = prop -> readReferenceProperty(context, prop, builder);
            } else {
                propertyGetter = n -> context.fieldMapper().fromFieldName(metaClass, n);
                propertyReader = prop -> readProperty(context, prop, builder);
            }

            PropertyMeta<T, ?> prop = propertyGetter.apply(name);
            if (prop != null) {
                if (context.reader().currentElement() == MetaElementType.Null) {
                    context.reader().readNull();
                } else {
                    propertyReader.accept(prop);
                }
                ++foundProperties;
            } else {
                context.reader().skipValue();
            }
        }
        context.reader().readEndObject();
        return (foundProperties > 0)
                ? builder.build()
                : null;
    }

    private <V> void writeProperty(MetaContext.Writer context, PropertyMeta<T, V> propertyMeta, T object) {
        V val = propertyMeta.getValue(object);
        if (val != null) {
            if (!alwaysEmbedNested && PropertyMetas.isReference(propertyMeta)) {
                context.writer().writeName(context.fieldMapper().toReferenceFieldName(propertyMeta));
                MetaClassWithKey<?, V> metaClass = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
                writeReference(context, metaClass, val);
            } else {
                context.writer().writeName(context.fieldMapper().toFieldName(propertyMeta));
                writeValue(context, propertyMeta.type(), val);
            }
        }
    }

    private <K, V> void writeReference(MetaContext.Writer context, MetaClassWithKey<K, V> metaClassWithKey, V value) {
        writeValue(context, metaClassWithKey.keyProperty().type(), metaClassWithKey.keyOf(value));
    }

    private <V> void writeValue(MetaContext.Writer context, TypeToken<V> valueType, V value) {
        MetaCodec<V> codec = context.codecProvider().resolve(valueType);
        codec.encode(context, value);
    }

    private <V> void readProperty(MetaContext.Reader context, PropertyMeta<T, V> propertyMeta, MetaBuilder<T> builder) {
        Optional.ofNullable(readValue(context, propertyMeta.type()))
                .ifPresent(val -> propertyMeta.setValue(builder, val));
    }

    private <V> V readValue(MetaContext.Reader context, TypeToken<V> type) {
        MetaCodec<V> codec = context.codecProvider().resolve(type);
        return codec.decode(context);
    }

    @SuppressWarnings("unchecked")
    private <V> void readReferenceProperty(MetaContext.Reader context, PropertyMeta<T, V> propertyMeta, MetaBuilder<T> builder) {
        MetaClassWithKey<?, V> metaClass = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
        Optional.ofNullable((V)readReference(context, (MetaClassWithKey)metaClass))
                .ifPresent(val -> propertyMeta.setValue(builder, val));
    }

    private <K, S extends HasMetaClassWithKey<K, S>> S readReference(MetaContext.Reader context, MetaClassWithKey<K, S> metaClass) {
        return Optional.ofNullable(readValue(context, metaClass.keyProperty().type()))
                .map(key -> resolver.resolve(metaClass, key))
                .orElse(null);
    }

    @AutoService(MetaCodecProvider.class)
    public static class Provider implements MetaCodecProvider {
        @Override
        public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
            return PropertyMetas.hasMetaClass(type)
                    ? new MetaClassCodec<>(MetaClasses.forTokenUnchecked(type), null)
                    : null;
        }
    }
}
