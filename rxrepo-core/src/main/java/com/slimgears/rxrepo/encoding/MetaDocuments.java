package com.slimgears.rxrepo.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.slimgears.rxrepo.encoding.adapter.JacksonAdapter;
import com.slimgears.rxrepo.encoding.codecs.MetaDocumentCodec;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.reflect.TypeToken;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MetaDocuments {
    public static MetaDocument create(MetaContext context) {
        return new DefaultDocument(context);
    }

    public static MetaDocument create() {
        return create(null);
    }

    public static class DefaultDocument implements MetaDocument {
        private final Map<String, Object> values = new HashMap<>();
        private final MetaContext context;

        private DefaultDocument(MetaContext metaContext) {
            this.context = metaContext;
        }

        @Override
        public Map<String, Object> asMap() {
            return values;
        }

        @Override
        public <V> MetaDocument set(String name, V value) {
            values.put(name, value);
            return this;
        }

        @Override
        public <V> V get(String name, TypeToken<V> type) {
            Object value = values.get(name);
            if (type.asClass().isInstance(value)) {
                return type.asClass().cast(value);
            }

            if (value == null) {
                return null;
            }

            if (context != null) {
                MetaCodec<V> codec = context.codecProvider().resolve(type);
                return codec.decode(context.ofReader(MetaReaders.fromObject(value)));
            } else {
                throw new MetaCodecException(MoreStrings
                        .format("Cannot decode object of type: {} to requested type: {}",
                                value.getClass().getName(),
                                type));
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MetaDocument && ((MetaDocument)obj).asMap().equals(asMap());
        }

        @Override
        public int hashCode() {
            return asMap().hashCode();
        }

        @Override
        public String toString() {
            StringWriter writer = new StringWriter();
            try (JsonGenerator generator = new JsonFactory().createGenerator(writer)) {
                MetaContext.Writer metaWriter = Optional.ofNullable(context)
                        .orElseGet(() -> MetaContexts.create(MetaCodecs.discover()))
                        .ofWriter(JacksonAdapter.writer(generator));
                new MetaDocumentCodec().encode(metaWriter, this);
                return writer.toString();
            } catch (IOException e) {
                return MoreStrings.format("{Error occurred: {}}", e.getMessage());
            }
        }
    }

    public static MetaReader toReader(MetaDocument doc) {
        return MetaReaders.fromDocument(doc);
    }

    public static <T extends HasMetaClass<T>> MetaDocument toDocument(T obj) {
        return toDocument(obj, obj.metaClass());
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromDocument(MetaDocument metaDocument, MetaClass<T> metaClass) {
        if (metaDocument == null) {
            return null;
        }
        MetaBuilder<T> builder = metaClass.createBuilder();
        metaClass.properties().forEach(p -> {
            if (PropertyMetas.hasMetaClass(p)) {
                p.setValue(builder, fromDocument(metaDocument.get(p.name(), MetaDocument.class), MetaClasses.forTokenUnchecked(p.type())));
            } else {
                ((PropertyMeta)p).setValue(builder, metaDocument.get(p.name(), p.type()));
            }
        });
        return builder.build();
    }

    private static <T> MetaDocument toDocument(T obj, MetaClass<T> metaClass) {
        if (obj == null) {
            return null;
        }

        MetaDocument doc = MetaDocument.create();
        metaClass.properties().forEach(p -> {
            if (PropertyMetas.hasMetaClass(p)) {
                MetaDocument childDoc = toDocument(p.getValue(obj), MetaClasses.forTokenUnchecked(p.type()));
                doc.set(p.name(), childDoc);
            } else {
                doc.set(p.name(), p.getValue(obj));
            }
        });
        return doc;
    }
}
