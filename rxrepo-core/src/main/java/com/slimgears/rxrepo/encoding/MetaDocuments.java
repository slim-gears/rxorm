package com.slimgears.rxrepo.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.slimgears.rxrepo.encoding.adapter.JacksonAdapter;
import com.slimgears.rxrepo.encoding.codecs.MetaDocumentCodec;
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
}
