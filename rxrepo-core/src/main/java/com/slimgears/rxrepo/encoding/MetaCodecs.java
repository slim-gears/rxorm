package com.slimgears.rxrepo.encoding;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings({"WeakerAccess", "UnstableApiUsage"})
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

    public static MetaCodecProvider fromProviders(MetaCodecProvider... registries) {
        return fromProviders(Arrays.asList(registries));
    }

    public static MetaCodecProvider fromProviders(Iterable<MetaCodecProvider> registries) {
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
        return fromAdapter(writer, Function.identity(), reader, Function.identity());
    }

    public static <T, R> MetaCodec<T> fromAdapter(BiConsumer<MetaWriter, R> writer,
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

        Iterable<MetaCodecProvider> fromLoader = ImmutableList.copyOf(ServiceLoader.load(
                MetaCodecProvider.class,
                ClassLoader.getSystemClassLoader()));

        return cachedOf(fromProviders(
                fromProviders(fromModules),
                fromProviders(fromLoader)));
    }

    public static MetaCodecProvider cachedOf(MetaCodecProvider provider) {
        return provider;
//        return new MetaCodecProvider() {
//            private final Map<TypeToken<?>, MetaCodec<?>> cache = new ConcurrentHashMap<>();
//
//            @SuppressWarnings("unchecked")
//            @Override
//            public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
//                return (MetaCodec<T>)cache.computeIfAbsent(type, provider::tryResolve);
//            }
//        };
    }

    public static <T> MetaCodec<T> longAdapter(Function<T, Long> toLong, Function<Long, T> fromLong) {
        return fromAdapter(MetaWriter::writeLong, toLong, MetaReader::readLong, fromLong);
    }

    public static <T> MetaCodec<T> intAdapter(Function<T, Integer> toInt, Function<Integer, T> fromInt) {
        return fromAdapter(MetaWriter::writeInt, toInt, MetaReader::readInt, fromInt);
    }

    public static <T> MetaCodec<T> doubleAdapter(Function<T, Double> toDouble, Function<Double, T> fromDouble) {
        return fromAdapter(MetaWriter::writeDouble, toDouble, MetaReader::readDouble, fromDouble);
    }

    public static <T> MetaCodec<T> stringAdapter(Function<T, String> toString, Function<String, T> fromString) {
        return fromAdapter(MetaWriter::writeString, toString, MetaReader::readString, fromString);
    }

    public static <T> MetaCodec<T> bytesAdapter(Function<T, byte[]> toBytes, Function<byte[], T> fromBytes) {
        return fromAdapter(MetaWriter::writeBytes, toBytes, MetaReader::readBytes, fromBytes);
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
        public <T> Builder add(TypeToken<T> type, Supplier<MetaCodec<T>> provider) {
            codecMapBuilder.put(type, (Supplier<MetaCodec<?>>)(Supplier<?>)provider);
            return this;
        }

        public <T> Builder add(TypeToken<T> type, MetaCodec<T> codec) {
            return add(type, () -> codec);
        }

        public <T> Builder add(Class<T> clazz, MetaCodec<T> codec) {
            return add(TypeToken.of(clazz), codec);
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
            return add(type, fromAdapter(writer, toWritable, reader, fromWritable));
        }

        public MetaCodecProvider build() {
            MetaCodecProvider registryFromList = fromProviders(registryListBuilder.build());
            Map<TypeToken<?>, Supplier<MetaCodec<?>>> codecMap = codecMapBuilder.build();
            MetaCodecProvider registryFromMap = new MetaCodecProvider() {
                @Override
                public <T> MetaCodec<T> tryResolve(TypeToken<T> type) {
                    TypeToken<MetaCodec<T>> codecType = TypeTokens.ofParameterized(MetaCodec.class, type);
                    return Optional
                            .ofNullable(codecMap.get(type))
                            .map(Supplier::get)
                            .map(TypeTokens.asClass(codecType)::cast)
                            .orElse(null);
                }
            };
            return cachedOf(fromProviders(registryFromMap, registryFromList));
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

        return Optionals.or(
                () -> Optional.ofNullable(provider.tryResolve(token))
                        .map(codec -> (MetaCodec<T>)codec),
                () -> Optional
                        .of(TypeTokens.asClass(token))
                        .map(TypeToken::of)
                        .filter(t -> !t.equals(token))
                        .map(provider::tryResolve)
                        .map(codec -> (MetaCodec<T>)codec),
                () -> Optional
                        .ofNullable(token.getRawType().getGenericSuperclass())
                        .map(TypeToken::of)
                        .map(superClass -> MetaCodecs.tryFindCodec(provider, superClass, visited)),
                () -> Arrays
                        .stream(TypeTokens.asClass(token).getGenericInterfaces())
                        .map(TypeToken::of)
                        .map(t -> MetaCodecs.<T>tryFindCodec(provider, t, visited))
                        .filter(Objects::nonNull)
                        .findFirst())
                .orElse(null);
    }
}
