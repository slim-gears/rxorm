package com.slimgears.rxrepo.encoding;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Streams;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class MetaCodecs {
    private final static MetaCodecProvider emptyProvider = new MetaCodecProvider() {
        @Override
        public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
            return null;
        }
    };

    public static Builder builder() {
        return new Builder();
    }

    public static MetaCodecProvider emptyProvider() {
        return emptyProvider;
    }

    public static MetaCodecProvider fromRegistries(MetaCodecProvider... registries) {
        return fromRegistries(Arrays.asList(registries));
    }

    public static MetaCodecProvider fromRegistries(Iterable<MetaCodecProvider> registries) {
        return new MetaCodecProvider() {
            @Override
            public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
                return Streams.fromIterable(registries)
                        .map(r -> r.tryResolve(type))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);
            }
        };
    }

    public static <T> MetaCodec<T> fromAdapter(TypeToken<T> type,
                                               BiConsumer<MetaWriter, T> writer,
                                               Function<MetaReader, T> reader) {
        return fromAdapter(type, writer, Function.identity(), reader, Function.identity());
    }

    public static <T, R> MetaCodec<T> fromAdapter(TypeToken<T> type,
                                                  BiConsumer<MetaWriter, R> writer,
                                                  Function<T, R> toWritable,
                                                  Function<MetaReader, R> reader,
                                                  Function<R, T> fromWritable) {
        return new MetaCodec<T>() {
            @Override
            public void encode(MetaContext.Writer context, T value) {
                writer.accept(context.writer(), toWritable.apply(value));
            }

            @Override
            public T decode(MetaContext.Reader context) {
                return fromWritable.apply(reader.apply(context.reader()));
            }
        };
    }

    public static MetaCodecProvider discover() {
        List<MetaCodecProvider> fromModules = Streams
                .fromIterable(ServiceLoader.load(
                        MetaCodecProvider.Module.class,
                        ClassLoader.getSystemClassLoader()))
                .map(MetaCodecProvider.Module::create)
                .collect(Collectors.toList());

        ServiceLoader<MetaCodecProvider> fromLoader = ServiceLoader.load(
                MetaCodecProvider.class,
                ClassLoader.getSystemClassLoader());

        return cachedOf(fromRegistries(
                fromRegistries(fromModules),
                fromRegistries(fromLoader)));
    }

    public static MetaCodecProvider cachedOf(MetaCodecProvider registry) {
        return new MetaCodecProvider() {
            private final Map<TypeToken<?>, MetaCodec<?>> cache = new ConcurrentHashMap<>();

            @SuppressWarnings("unchecked")
            @Override
            public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
                return (MetaCodec<T>)cache.computeIfAbsent(type, registry::tryResolve);
            }
        };
    }

    public static <T> MetaCodec<T> longAdapter(TypeToken<T> type, Function<T, Long> toLong, Function<Long, T> fromLong) {
        return fromAdapter(type, MetaWriter::writeLong, toLong, MetaReader::readLong, fromLong);
    }

    public static <T> MetaCodec<T> intAdapter(TypeToken<T> type, Function<T, Integer> toInt, Function<Integer, T> fromInt) {
        return fromAdapter(type, MetaWriter::writeInt, toInt, MetaReader::readInt, fromInt);
    }

    public static <T> MetaCodec<T> doubleAdapter(TypeToken<T> type, Function<T, Double> toDouble, Function<Double, T> fromDouble) {
        return fromAdapter(type, MetaWriter::writeDouble, toDouble, MetaReader::readDouble, fromDouble);
    }

    public static <T> MetaCodec<T> stringAdapter(TypeToken<T> type, Function<T, String> toString, Function<String, T> fromString) {
        return fromAdapter(type, MetaWriter::writeString, toString, MetaReader::readString, fromString);
    }

    public static <T> MetaCodec<T> bytesAdapter(TypeToken<T> type, Function<T, byte[]> toBytes, Function<byte[], T> fromBytes) {
        return fromAdapter(type, MetaWriter::writeBytes, toBytes, MetaReader::readBytes, fromBytes);
    }

    public static class Builder {
        private final ImmutableMap.Builder<TypeToken<?>, Supplier<MetaCodec<?>>> codecMapBuilder = ImmutableMap.builder();
        private final ImmutableList.Builder<MetaCodecProvider> registryListBuilder = ImmutableList.builder();

        public Builder discover() {
            return add(MetaCodecs.discover());
        }

        public Builder add(MetaCodecProvider.Module module) {
            return add(module.create());
        }

        public Builder add(MetaCodecProvider registry) {
            registryListBuilder.add(registry);
            return this;
        }

        public <T> Builder add(Predicate<TypeToken<?>> typePredicate, Function<TypeToken<T>, MetaCodec<T>> provider) {
            return add(new Entry<>(typePredicate, provider).toRegistry());
        }

        @SuppressWarnings("unchecked")
        public <T> Builder add(TypeToken<?> type, Supplier<MetaCodec<T>> provider) {
            codecMapBuilder.put(type, (Supplier<MetaCodec<?>>)(Supplier<?>)provider);
            return this;
        }

        public <T> Builder add(TypeToken<?> type, MetaCodec<T> codec) {
            return add(type, () -> codec);
        }

        public <T> Builder add(TypeToken<T> type, BiConsumer<MetaWriter, T> writer, Function<MetaReader, T> reader) {
            return add(type, fromAdapter(type, writer, reader));
        }

        public <T> Builder add(Class<T> type, BiConsumer<MetaWriter, T> writer, Function<MetaReader, T> reader) {
            return add(TypeToken.of(type), writer, reader);
        }

        public <T, R> Builder adapter(TypeToken<T> type,
                                  BiConsumer<MetaWriter, R> writer,
                                  Function<T, R> toWritable,
                                  Function<MetaReader, R> reader,
                                  Function<R, T> fromWritable) {
            return add(type, fromAdapter(type, writer, toWritable, reader, fromWritable));
        }

        public MetaCodecProvider build() {
            MetaCodecProvider registryFromList = fromRegistries(registryListBuilder.build());
            Map<TypeToken<?>, Supplier<MetaCodec<?>>> codecMap = codecMapBuilder.build();
            MetaCodecProvider registryFromMap = new MetaCodecProvider() {
                @Override
                public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
                    TypeToken<MetaCodec<T>> codecType = TypeToken.ofParameterized(MetaCodec.class, type);
                    return Optional
                            .ofNullable(codecMap.get(type))
                            .map(Supplier::get)
                            .map(codecType.asClass()::cast)
                            .orElse(null);
                }
            };
            return cachedOf(fromRegistries(registryFromMap, registryFromList));
        }

        static class Entry<T> {
            private final Predicate<TypeToken<?>> typePredicate;
            private final Function<TypeToken<T>, MetaCodec<T>> codecProvider;

            Entry(Predicate<TypeToken<?>> typePredicate, Function<TypeToken<T>, MetaCodec<T>> codecProvider) {
                this.typePredicate = typePredicate;
                this.codecProvider = codecProvider;
            }

            @SuppressWarnings("unchecked")
            private MetaCodec<T> provideIfSupported(TypeToken<?> typeToken) {
                return typePredicate.test(typeToken)
                        ? codecProvider.apply((TypeToken<T>)typeToken)
                        : null;
            }

            MetaCodecProvider toRegistry() {
                return new MetaCodecProvider() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public <_T> MetaCodec<_T> tryResolve(TypeToken<_T> type) {
                        return (MetaCodec<_T>)provideIfSupported(type);
                    }
                };
            }
        }
    }

    public static <T> MetaCodec<T> tryFindCodec(MetaCodecProvider provider, TypeToken<?> token) {
        return tryFindCodec(provider, token, new HashSet<>());
    }

    @SuppressWarnings("unchecked")
    private static <T> MetaCodec<T> tryFindCodec(MetaCodecProvider provider, TypeToken<?> token, Set<TypeToken<?>> visited) {
        if (!visited.add(token)) {
            return null;
        }

        return Optional
                .ofNullable(provider.tryResolve(token))
                .map(codec -> (MetaCodec<T>)codec)
                .orElseGet(() -> Optional
                        .ofNullable(token.asClass().getSuperclass())
                        .map(TypeToken::of)
                        .map(superClass -> MetaCodecs.<T>tryFindCodec(provider, superClass, visited))
                        .orElseGet(() -> Arrays
                                .stream(token.asClass().getGenericInterfaces())
                                .map(TypeToken::ofType)
                                .map(t -> MetaCodecs.<T>tryFindCodec(provider, t, visited))
                                .filter(Objects::nonNull)
                                .findFirst()
                                .orElse(null)));
    }
}
