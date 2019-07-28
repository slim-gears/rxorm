package com.slimgears.rxrepo.mongodb.codecs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Function;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

@SuppressWarnings("WeakerAccess")
public class Codecs {
    public static <T> Codec<T> longCodec(Class<T> cls, Function<T, Long> toLong, Function<Long, T> fromLong) {
        return codec(cls, toLong, BsonReader::readInt64, fromLong, BsonWriter::writeInt64);
    }

    public static <T> Codec<T> stringCodec(Class<T> cls, Function<T, String> toString, Function<String, T> fromString) {
        return codec(cls, toString, BsonReader::readString, fromString, BsonWriter::writeString);
    }

    public static <E extends Enum<E>> Codec<E> enumCodec(Class<E> cls) {
        return stringCodec(cls, E::name, val -> Enum.valueOf(cls, val));
    }

    public static CodecProvider discoverProviders() {
        CodecProviderBuilder builder = providerBuilder();
        ServiceLoader.load(CodecProvider.class, ClassLoader.getSystemClassLoader())
                .forEach(builder::provider);
        return builder.build();
    }

    public static CodecRegistry discover() {
        List<CodecRegistry> discoveredRegistries = ImmutableList.copyOf(ServiceLoader.load(CodecRegistry.class, ClassLoader.getSystemClassLoader()));
        CodecRegistry discoveredProviders = CodecRegistries.fromProviders(discoverProviders());
        return !discoveredRegistries.isEmpty()
                ? CodecRegistries.fromRegistries(
                        discoveredProviders,
                        CodecRegistries.fromRegistries(discoveredRegistries))
                : discoveredProviders;
    }

    public static CodecProviderBuilder providerBuilder() {
        return new CodecProviderBuilder();
    }

    private static <T> CodecProvider providerForCodec(Class<T> cls, Codec<T> codec) {
        return new CodecProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public <_T> Codec<_T> get(Class<_T> clazz, CodecRegistry registry) {
                return clazz == cls ? (Codec<_T>)codec : null;
            }
        };
    }

    public static class CodecProviderBuilder {
        private final ImmutableMap.Builder<Class<?>, CodecProvider> codecMapBuilder = ImmutableMap.builder();
        private final ImmutableList.Builder<CodecProvider> codecProviderListBuilder = ImmutableList.builder();

        public <T> CodecProviderBuilder codec(Class<T> cls, Codec<T> codec) {
            codecMapBuilder.put(cls, providerForCodec(cls, codec));
            return this;
        }

        public CodecProviderBuilder provider(CodecProvider provider) {
            return providers(provider);
        }

        public CodecProviderBuilder providers(CodecProvider... providers) {
            codecProviderListBuilder.add(providers);
            return this;
        }

        public <T> CodecProviderBuilder documentCodec(Class<T> cls, Function<T, Document> toDocument, Function<Document, T> fromDocument) {
            codecMapBuilder.put(cls, new CodecProvider() {
                @SuppressWarnings("unchecked")
                @Override
                public <_T> Codec<_T> get(Class<_T> clazz, CodecRegistry registry) {
                    return (Codec<_T>)Codecs.documentCodec(cls, toDocument, fromDocument, registry.get(Document.class));
                }
            });
            return this;
        }

        public <T> CodecProviderBuilder stringCodec(Class<T> cls, Function<T, String> toString, Function<String, T> fromString) {
            return codec(cls, Codecs.stringCodec(cls, toString, fromString));
        }

        public <T> CodecProviderBuilder longCodec(Class<T> cls, Function<T, Long> toLong, Function<Long, T> fromLong) {
            return codec(cls, Codecs.longCodec(cls, toLong, fromLong));
        }

        public CodecProvider build() {
            ImmutableMap<Class<?>, CodecProvider> codecMap = codecMapBuilder.build();
            ImmutableList<CodecProvider> providers = codecProviderListBuilder.build();
            return new CodecProvider() {
                @Override
                public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
                    return Optional
                            .ofNullable(codecMap.get(clazz))
                            .map(cp -> cp.get(clazz, registry))
                            .orElseGet(() -> providers
                                    .stream()
                                    .map(p -> p.get(clazz, registry))
                                    .filter(Objects::nonNull)
                                    .findFirst()
                                    .orElse(null));
                }
            };
        }
    }

    private static <T, R> Codec<T> codec(Class<T> cls,
                                         Function<T, R> toValue,
                                         Function<BsonReader, R> reader,
                                         Function<R, T> fromValue,
                                         BiConsumer<BsonWriter, R> writer) {
        return new Codec<T>() {
            @Override
            public T decode(BsonReader _reader, DecoderContext decoderContext) {
                try {
                    R val = reader.apply(_reader);
                    return fromValue.apply(val);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void encode(BsonWriter _writer, T value, EncoderContext encoderContext) {
                try {
                    writer.accept(_writer, toValue.apply(value));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Class<T> getEncoderClass() {
                return cls;
            }
        };
    }

    private static <T> Codec<T> documentCodec(Class<T> cls, Function<T, Document> toDocument, Function<Document, T> fromDocument, Codec<Document> documentCodec) {
        return new Codec<T>() {
            @Override
            public T decode(BsonReader reader, DecoderContext decoderContext) {
                Document doc = documentCodec.decode(reader, decoderContext);
                try {
                    return fromDocument.apply(doc);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
                try {
                    Document doc = toDocument.apply(value);
                    documentCodec.encode(writer, doc, encoderContext);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Class<T> getEncoderClass() {
                return cls;
            }
        };
    }
}
