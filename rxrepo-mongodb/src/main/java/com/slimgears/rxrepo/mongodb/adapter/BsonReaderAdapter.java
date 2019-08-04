package com.slimgears.rxrepo.mongodb.adapter;

import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.encoding.MetaElementType;
import com.slimgears.rxrepo.encoding.MetaReader;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Optionals;
import org.bson.AbstractBsonReader;
import org.bson.BsonReader;
import org.bson.BsonType;

import java.util.Optional;

public class BsonReaderAdapter implements MetaReader {
    private final static ImmutableMap<BsonType, MetaElementType> typeMap = ImmutableMap
            .<BsonType, MetaElementType>builder()
            .put(BsonType.DOCUMENT, MetaElementType.BeginObject)
            .put(BsonType.END_OF_DOCUMENT, MetaElementType.EndObject)
            .put(BsonType.ARRAY, MetaElementType.BeginArray)
            .put(BsonType.INT32, MetaElementType.Integer)
            .put(BsonType.INT64, MetaElementType.Long)
            .put(BsonType.DOUBLE, MetaElementType.Double)
            .put(BsonType.BOOLEAN, MetaElementType.Boolean)
            .put(BsonType.STRING, MetaElementType.String)
            .put(BsonType.NULL, MetaElementType.Null)
            .build();

    private final static ImmutableMap<AbstractBsonReader.State, MetaElementType> stateMap = ImmutableMap
            .<AbstractBsonReader.State, MetaElementType>builder()
            .put(AbstractBsonReader.State.END_OF_DOCUMENT, MetaElementType.EndObject)
            .put(AbstractBsonReader.State.END_OF_ARRAY, MetaElementType.EndArray)
            .put(AbstractBsonReader.State.DONE, MetaElementType.End)
            .build();

    private final BsonReader reader;

    private BsonReaderAdapter(BsonReader reader) {
        this.reader = reader;
    }

    static MetaReader forBsonReader(BsonReader reader) {
        return new BsonReaderAdapter(reader);
    }

    @Override
    public MetaElementType currentElement() {
        return Optionals.or(
                () -> Optional.of(reader)
                        .flatMap(Optionals.ofType(AbstractBsonReader.class))
                        .map(r -> {
                            if (r.getState() == AbstractBsonReader.State.TYPE) {
                                r.readBsonType();
                            }
                            return r.getState();
                        })
                        .map(stateMap::get),
                () -> Optional.ofNullable(typeMap.get(reader.getCurrentBsonType())))
                .orElseThrow(() -> new IllegalStateException(MoreStrings.format("Could not recognize current element (state: {}, type: {})",
                        reader instanceof AbstractBsonReader ? ((AbstractBsonReader) reader).getState() : null,
                        reader.getCurrentBsonType())));
    }

    @Override
    public void readBeginObject() {
        reader.readStartDocument();
    }

    @Override
    public void readEndObject() {
        reader.readEndDocument();
    }

    @Override
    public void readBeginArray() {
        reader.readStartArray();
    }

    @Override
    public void readEndArray() {
        reader.readEndArray();
    }

    @Override
    public String readName() {
        return reader.readName();
    }

    @Override
    public long readLong() {
        return reader.readInt64();
    }

    @Override
    public int readInt() {
        return reader.readInt32();
    }

    @Override
    public short readShort() {
        return (short)readInt();
    }

    @Override
    public float readFloat() {
        return (float)readDouble();
    }

    @Override
    public double readDouble() {
        return reader.readDouble();
    }

    @Override
    public boolean readBoolean() {
        return reader.readBoolean();
    }

    @Override
    public String readString() {
        return reader.readString();
    }

    @Override
    public void readNull() {
        reader.readNull();
    }

    @Override
    public byte[] readBytes() {
        return reader.readBinaryData().getData();
    }

    @Override
    public void skipValue() {
        reader.skipValue();
    }
}
