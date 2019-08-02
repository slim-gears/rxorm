package com.slimgears.rxrepo.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.encoding.adapter.JacksonAdapter;
import com.slimgears.rxrepo.queries.TestEntity;
import com.slimgears.rxrepo.queries.TestEntityPrototype;
import com.slimgears.rxrepo.queries.TestKey;
import com.slimgears.rxrepo.queries.TestRefEntity;
import com.slimgears.util.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

public class MetaClassCodecTest {
    private final MetaCodecProvider codecProvider = MetaCodecs.builder()
            .discover()
            .add(TypeToken.of(TestKey.class), MetaCodecs.stringAdapter(TestKey::name, TestKey::create))
            .build();
    private final MetaContext context = MetaContexts.create(codecProvider);
    private final JsonFactory factory = new JsonFactory();
    private final TestEntity testEntity = TestEntity
            .builder()
            .address("Address")
            .key(TestKey.create("Key1"))
            .number(10)
            .text("Text10")
            .refEntityId(2)
            .refEntityText("Text1")
            .enumVal(TestEntityPrototype.TestEnum.Val2)
            .refEntities(ImmutableList.of(TestRefEntity.create(4, "Test4")))
            .build();

    @Test
    public void testMetaObjectEncodingDecoding() throws IOException {
        StringWriter stringWriter = new StringWriter();
        try (JsonGenerator generator = factory.createGenerator(stringWriter)) {
            context.write(JacksonAdapter.writer(generator), testEntity);
        }
        String json = stringWriter.toString();
        Assert.assertEquals("{\"key\":\"Key1\",\"text\":\"Text10\",\"number\":10,\"refEntity\":{\"id\":2,\"text\":\"Text1\"},\"refEntities\":[{\"id\":4,\"text\":\"Test4\"}],\"address\":\"Address\",\"enumVal\":\"Val2\",\"__text\":\"TestKey{name=Key1} Text10\"}", json);
        try (JsonParser parser = factory.createParser(json)) {
            TestEntity decodedEntity = context.read(JacksonAdapter.reader(parser), TestEntity.metaClass.objectClass());
            Assert.assertEquals(testEntity, decodedEntity);
        }
    }

    @Test
    public void testToDocumentFromDocument() {
        MetaDocument doc = MetaDocuments.toDocument(testEntity);
        TestEntity decodedEntity = MetaDocuments.fromDocument(doc, TestEntity.metaClass);
        Assert.assertEquals(testEntity, decodedEntity);
    }
}
