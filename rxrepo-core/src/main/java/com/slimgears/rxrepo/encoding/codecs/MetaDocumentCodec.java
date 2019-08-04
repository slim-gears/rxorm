package com.slimgears.rxrepo.encoding.codecs;

import com.google.auto.service.AutoService;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.util.reflect.TypeTokens;

import java.util.Map;

public class MetaDocumentCodec implements MetaCodec<MetaDocument> {
    @Override
    public void encode(MetaContext.Writer context, MetaDocument doc) {
        Map<String, Object> map = doc.asMap();
        context.writer().writeBeginObject();
        map.forEach((key, value) -> {
            context.writer().writeName(key);
            if (value == null) {
                context.writer().writeNull();
            } else {
                MetaCodec<Object> codec = context
                        .codecProvider()
                        .resolve(TypeTokens.ofType(value.getClass()));
                codec.encode(context, value);
            }
        });
        context.writer().writeEndObject();
    }

    @Override
    public MetaDocument decode(MetaContext.Reader context) {
        MetaDocument doc = MetaDocuments.create(context);
        MetaReader reader = context.reader();
        reader.readBeginObject();
        while (!reader.isAt(MetaElementType.EndObject)) {
            String name = reader.readName();
            if (reader.isAt(MetaElementType.BeginObject)) {
                doc.set(name, decodeDocument(context));
            } else if (reader.isAt(MetaElementType.BeginArray)) {
                doc.set(name, decodeIterable(context));
            } else {
                doc.set(name, decodeValue(context));
            }
        }
        reader.readEndObject();
        return doc;
    }

    private Iterable<?> decodeIterable(MetaContext.Reader context) {
        MetaCodec<Iterable<?>> codec = context.codecProvider().resolve(TypeTokens.ofType(Iterable.class));
        return codec.decode(context);
    }

    private MetaDocument decodeDocument(MetaContext.Reader context) {
        return decode(context);
    }

    private Object decodeValue(MetaContext.Reader context) {
        return context.reader().readValue();
    }

    @AutoService(Provider.class)
    public static class Provider implements MetaCodecProvider {

        @SuppressWarnings("unchecked")
        @Override
        public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
            return type.getRawType() == MetaDocument.class
                    ? (MetaCodec<T>)new MetaDocumentCodec()
                    : null;
        }
    }
}
