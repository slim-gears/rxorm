package com.slimgears.rxrepo.encoding.adapter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.encoding.MetaElementType;
import com.slimgears.rxrepo.encoding.MetaReader;
import com.slimgears.rxrepo.encoding.MetaWriter;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Safe;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class JacksonAdapter {
    private final static ImmutableMap<JsonToken, MetaElementType> jsonTokenToElementType = ImmutableMap
            .<JsonToken, MetaElementType>builder()
            .put(JsonToken.START_ARRAY, MetaElementType.BeginArray)
            .put(JsonToken.END_ARRAY, MetaElementType.EndArray)
            .put(JsonToken.START_OBJECT, MetaElementType.BeginObject)
            .put(JsonToken.END_OBJECT, MetaElementType.EndObject)
            .put(JsonToken.FIELD_NAME, MetaElementType.Name)
            .put(JsonToken.VALUE_FALSE, MetaElementType.Boolean)
            .put(JsonToken.VALUE_NULL, MetaElementType.Null)
            .put(JsonToken.VALUE_NUMBER_FLOAT, MetaElementType.Double)
            .put(JsonToken.VALUE_NUMBER_INT, MetaElementType.Long)
            .put(JsonToken.VALUE_STRING, MetaElementType.String)
            .build();

    public static MetaWriter writer(JsonGenerator generator) {
        return new MetaWriter() {
            @Override
            public MetaWriter writeBeginObject() {
                return invoke(JsonGenerator::writeStartObject);
            }

            @Override
            public MetaWriter writeEndObject() {
                return invoke(JsonGenerator::writeEndObject);
            }

            @Override
            public MetaWriter writeBeginArray() {
                return invoke(JsonGenerator::writeStartArray);
            }

            @Override
            public MetaWriter writeEndArray() {
                return invoke(JsonGenerator::writeEndArray);
            }

            @Override
            public MetaWriter writeName(String name) {
                return invoke(g -> g.writeFieldName(name));
            }

            @Override
            public MetaWriter writeLong(long value) {
                return invoke(g -> g.writeNumber(value));
            }

            @Override
            public MetaWriter writeInt(int value) {
                return invoke(g -> g.writeNumber(value));
            }

            @Override
            public MetaWriter writeShort(short value) {
                return invoke(g -> g.writeNumber(value));
            }

            @Override
            public MetaWriter writeFloat(float value) {
                return invoke(g -> g.writeNumber(value));
            }

            @Override
            public MetaWriter writeDouble(double value) {
                return invoke(g -> g.writeNumber(value));
            }

            @Override
            public MetaWriter writeBoolean(boolean value) {
                return invoke(g -> g.writeBoolean(value));
            }

            @Override
            public MetaWriter writeString(String value) {
                return invoke(g -> g.writeString(value));
            }

            @Override
            public MetaWriter writeNull() {
                return invoke(JsonGenerator::writeNull);
            }

            @Override
            public MetaWriter writeBytes(byte[] bytes) {
                return invoke(g -> g.writeBinary(bytes));
            }

            @Override
            public MetaWriter writeValue(Object object) {
                return invoke(g -> g.writeObject(object));
            }

            private MetaWriter invoke(Safe.UnsafeConsumer<JsonGenerator> invocation) {
                try {
                    invocation.accept(generator);
                    return this;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static MetaReader reader(JsonParser jsonParser) {
        try {
            jsonParser.nextToken();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new MetaReader() {
            @Override
            public MetaElementType currentElement() {
                return Optional.ofNullable(jsonTokenToElementType.get(jsonParser.currentToken()))
                        .orElse(MetaElementType.End);
            }

            @Override
            public void readBeginObject() {
                readToken(JsonToken.START_OBJECT);
            }

            @Override
            public void readEndObject() {
                readToken(JsonToken.END_OBJECT);
            }

            @Override
            public void readBeginArray() {
                readToken(JsonToken.START_ARRAY);
            }

            @Override
            public void readEndArray() {
                readToken(JsonToken.END_ARRAY);
            }

            @Override
            public String readName() {
                return readToken(JsonParser::currentName, JsonToken.FIELD_NAME);
            }

            @Override
            public long readLong() {
                return readToken(JsonParser::getLongValue, JsonToken.VALUE_NUMBER_INT);
            }

            @Override
            public int readInt() {
                return readToken(JsonParser::getIntValue, JsonToken.VALUE_NUMBER_INT);
            }

            @Override
            public short readShort() {
                return readToken(JsonParser::getShortValue, JsonToken.VALUE_NUMBER_INT);
            }

            @Override
            public float readFloat() {
                return readToken(JsonParser::getFloatValue, JsonToken.VALUE_NUMBER_FLOAT);
            }

            @Override
            public double readDouble() {
                return readToken(JsonParser::getDoubleValue, JsonToken.VALUE_NUMBER_FLOAT);
            }

            @Override
            public boolean readBoolean() {
                return readToken(JsonParser::getBooleanValue, JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE);
            }

            @Override
            public String readString() {
                return readToken(JsonParser::getText, JsonToken.VALUE_STRING);
            }

            @Override
            public void readNull() {
                readToken(JsonToken.VALUE_NULL);
            }

            @Override
            public byte[] readBytes() {
                return readToken(JsonParser::getBinaryValue, JsonToken.VALUE_STRING);
            }

            @Override
            public void skipValue() {
                Safe.ofRunnable(jsonParser::nextValue).run();
            }

            private void readToken(JsonToken... expectedTokens) {
                this.<Void>readToken(p -> null, expectedTokens);
            }

            private <T> T readToken(Safe.UnsafeFunction<JsonParser, T> invocation, JsonToken... expectedTokens) {
                if (!Arrays.asList(expectedTokens).contains(jsonParser.currentToken())) {
                    throw new IllegalStateException(MoreStrings.format("Expected one of: ({}), actual token: {}",
                            Arrays.stream(expectedTokens).map(Objects::toString).collect(Collectors.joining(", ")),
                            jsonParser.currentToken()));
                }
                try {
                    T res = invocation.apply(jsonParser);
                    jsonParser.nextToken();
                    return res;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
