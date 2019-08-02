package com.slimgears.rxrepo.encoding;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.slimgears.util.generic.MoreStrings;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.slimgears.util.generic.LazyString.lazy;

@SuppressWarnings("WeakerAccess")
public class MetaReaders {
    private final static ImmutableBiMap<Class<?>, MetaElementType> classToElementType = ImmutableBiMap
            .<Class<?>, MetaElementType>builder()
            .put(Integer.class, MetaElementType.Integer)
            .put(Long.class, MetaElementType.Long)
            .put(Short.class, MetaElementType.Short)
            .put(String.class, MetaElementType.String)
            .put(Float.class, MetaElementType.Float)
            .put(Double.class, MetaElementType.Double)
            .put(byte[].class, MetaElementType.Binary)
            .build();

    private final static ImmutableBiMap<MetaElementType, Class<?>> elementTypeToClass = classToElementType.inverse();
    private final static ImmutableSet<MetaElementType> valueElements = ImmutableSet.
            <MetaElementType>builder()
            .add(MetaElementType.Integer, MetaElementType.Long, MetaElementType.Short)
            .add(MetaElementType.Float, MetaElementType.Double)
            .add(MetaElementType.Boolean)
            .add(MetaElementType.String)
            .add(MetaElementType.Binary)
            .add(MetaElementType.Null)
            .build();

    static Object readValue(MetaReader reader) {
        switch (reader.currentElement()) {
            case Integer: return reader.readInt();
            case Long: return reader.readLong();
            case Short: return reader.readShort();
            case Float: return reader.readFloat();
            case Double: return reader.readDouble();
            case String: return reader.readString();
            case Boolean: return reader.readBoolean();
            case Null:
                reader.readNull();
                return null;
            case Binary: return reader.readBytes();
            default:
                throw new IllegalStateException("Unrecognized value token: " + reader.currentElement());
        }
    }

    static MetaReader fromIterable(Iterable<?> iterable) {
        return new ArrayReader(iterable.iterator());
    }

    static MetaReader fromDocument(MetaDocument document) {
        return fromMap(document.asMap());
    }

    static MetaReader fromMap(Map<String, Object> map) {
        return new MapReader(map);
    }

    static MetaReader fromObject(Object object) {
        if (object instanceof Iterable) {
            return fromIterable((Iterable<?>)object);
        } else if (object instanceof MetaDocument) {
            return fromDocument((MetaDocument)object);
        } else {
            return new ValueReader(object);
        }
    }

    public static abstract class AbstractReader implements MetaReader {
        private final MetaReader defaultDelegate = new MetaReader() {
            @Override
            public MetaElementType currentElement() {
                return onCurrentElement();
            }

            @Override
            public String readName() {
                validateState(MetaElementType.Name);
                return onReadName();
            }

            @Override
            public void readBeginObject() {
                validateState(MetaElementType.BeginObject);
            }

            @Override
            public void readEndObject() {
                validateState(MetaElementType.EndObject);
            }

            @Override
            public void readBeginArray() {
                validateState(MetaElementType.BeginArray);
            }

            @Override
            public void readEndArray() {
                validateState(MetaElementType.EndArray);
            }

            @Override
            public long readLong() {
                return readAndValidate(MetaElementType.Long);
            }

            @Override
            public int readInt() {
                return readAndValidate(MetaElementType.Integer);
            }

            @Override
            public short readShort() {
                return readAndValidate(MetaElementType.Short);
            }

            @Override
            public float readFloat() {
                return readAndValidate(MetaElementType.Float);
            }

            @Override
            public double readDouble() {
                return readAndValidate(MetaElementType.Double);
            }

            @Override
            public boolean readBoolean() {
                return readAndValidate(MetaElementType.Boolean);
            }

            @Override
            public String readString() {
                return readAndValidate(MetaElementType.String);
            }

            @Override
            public void readNull() {
                readAndValidate(MetaElementType.Null);
            }

            @Override
            public byte[] readBytes() {
                return readAndValidate(MetaElementType.Binary);
            }

            @Override
            public Object readValue() {
                if (isFinished()) {
                    throw new MetaCodecException("Cannot read after end");
                }

                if (currentElement() == MetaElementType.Null) {
                    return null;
                }

                if (!valueElements.contains(currentElement())) {
                    throw new MetaCodecException(MoreStrings.format("Current element ({}) is not value element", lazy(this::currentElement)));
                }
                return onReadValue();
            }

            @Override
            public void skipValue() {
                readValue();
            }
        };

        @Override
        public MetaElementType currentElement() {
            return invokeDelegate(MetaReader::currentElement);
        }

        @Override
        public void readBeginObject() {
            invokeDelegate(MetaReader::readBeginObject);
        }

        @Override
        public void readEndObject() {
            invokeDelegate(MetaReader::readEndObject);
        }

        @Override
        public void readBeginArray() {
            invokeDelegate(MetaReader::readBeginArray);
        }

        @Override
        public void readEndArray() {
            invokeDelegate(MetaReader::readEndArray);
        }

        @Override
        public String readName() {
            return invokeDelegate(MetaReader::readName);
        }

        @Override
        public long readLong() {
            return invokeDelegate(MetaReader::readLong);
        }

        @Override
        public int readInt() {
            return invokeDelegate(MetaReader::readInt);
        }

        @Override
        public short readShort() {
            return invokeDelegate(MetaReader::readShort);
        }

        @Override
        public float readFloat() {
            return invokeDelegate(MetaReader::readFloat);
        }

        @Override
        public double readDouble() {
            return invokeDelegate(MetaReader::readDouble);
        }

        @Override
        public boolean readBoolean() {
            return invokeDelegate(MetaReader::readBoolean);
        }

        @Override
        public String readString() {
            return invokeDelegate(MetaReader::readString);
        }

        @Override
        public void readNull() {
            invokeDelegate(MetaReader::readNull);
        }

        @Override
        public byte[] readBytes() {
            return invokeDelegate(MetaReader::readBytes);
        }

        @Override
        public Object readValue() {
            return invokeDelegate(MetaReader::readValue);
        }

        @Override
        public void skipValue() {
             invokeDelegate(MetaReader::skipValue);
        }

        private final AtomicReference<MetaReader> delegate = new AtomicReference<>(defaultDelegate);

        protected abstract MetaElementType onCurrentElement();
        protected abstract Object onReadValue();
        protected abstract String onReadName();
        protected abstract void onDelegateEnd();

        protected void delegateTo(MetaReader delegate) {
            this.delegate.set(delegate);
        }

        private <T> T invokeDelegate(Function<MetaReader, T> invocation) {
            MetaReader reader = delegate.get();
            T result = invocation.apply(reader);
            if (reader.currentElement() == MetaElementType.End) {
                delegateTo(defaultDelegate);
                onDelegateEnd();
            }
            return result;
        }

        private void invokeDelegate(Consumer<MetaReader> invocation) {
             this.<Void>invokeDelegate(reader -> {
                invocation.accept(reader);
                return null;
            });
        }

        @SuppressWarnings("unchecked")
        private <T> T readAndValidate(MetaElementType expectedType) {
            validateState(expectedType);
            return (T)readValue();
        }

        private void validateState(MetaElementType expectedType) {
            if (expectedType != currentElement()) {
                illegalElementType(expectedType, currentElement());
            }
        }
    }

    private static class MapReader extends AbstractReader {
        private final Iterator<Map.Entry<String, Object>> entryIterator;
        private MetaElementType state = MetaElementType.BeginObject;

        public MapReader(Map<String, Object> map) {
            this.entryIterator = map.entrySet().iterator();
        }

        @Override
        public void readEndObject() {
            super.readEndObject();
            state = MetaElementType.End;
        }

        @Override
        protected MetaElementType onCurrentElement() {
            return state;
        }

        @Override
        protected Object onReadValue() {
            throw new MetaCodecException(MoreStrings.format("Cannot read value in state: {}", currentElement()));
        }

        @Override
        protected String onReadName() {
            Map.Entry<String, Object> entry = entryIterator.next();
            delegateTo(fromObject(entry.getValue()));
            return entry.getKey();
        }

        @Override
        protected void onDelegateEnd() {
            if (entryIterator.hasNext()) {
                state = MetaElementType.Name;
            } else {
                state = MetaElementType.EndObject;
            }
        }
    }

    private static class ArrayReader extends AbstractReader {
        private final Iterator<?> iterator;
        private MetaElementType state = MetaElementType.BeginArray;

        private ArrayReader(Iterator<?> iterator) {
            this.iterator = iterator;
        }

        @Override
        public void readBeginArray() {
            super.readBeginArray();
            onDelegateEnd();
        }

        @Override
        protected Object onReadValue() {
            throw new MetaCodecException(MoreStrings.format("Cannot read value at state: ", currentElement()));
        }

        @Override
        protected String onReadName() {
            throw new MetaCodecException(MoreStrings.format("Cannot read name at state: ", currentElement()));
        }

        @Override
        protected MetaElementType onCurrentElement() {
            return state;
        }

        protected void onDelegateEnd() {
            if (!iterator.hasNext()) {
                state = MetaElementType.EndArray;
            } else {
                Object obj = iterator.next();
                delegateTo(fromObject(obj));
            }
        }
    }

    private static class ValueReader extends AbstractReader {
        private final Object object;
        private MetaElementType elementType;

        private ValueReader(Object object) {
            this.object = object;
            this.elementType = toElementType(object);
        }

        @Override
        protected MetaElementType onCurrentElement() {
            return elementType;
        }

        @Override
        public String readName() {
            return illegalElementType(MetaElementType.Name, elementType);
        }

        @Override
        protected Object onReadValue() {
            this.elementType = MetaElementType.End;
            return object;
        }

        @Override
        protected String onReadName() {
            throw new MetaCodecException("Cannot read name in state: " + currentElement());
        }

        @Override
        protected void onDelegateEnd() {
        }
    }

    private static <T> T illegalElementType(MetaElementType expected, MetaElementType actual) {
        throw new IllegalStateException(MoreStrings.format("Actual element type ({}) does not match to expected ({})", actual, expected));
    }


    private static MetaElementType toElementType(Object object) {
        if (object == null) {
            return MetaElementType.Null;
        }

        return Optional
                .ofNullable(classToElementType.get(object.getClass()))
                .orElse(MetaElementType.Binary);
    }
}
