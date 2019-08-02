package com.slimgears.rxrepo.core.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.slimgears.rxrepo.encoding.*;
import com.slimgears.rxrepo.encoding.adapter.JacksonAdapter;
import com.slimgears.rxrepo.encoding.codecs.MetaDocumentCodec;
import com.slimgears.rxrepo.encoding.codecs.StandardCodecModule;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

public class MetaReaderWriterTest {
    @Test
    public void testWriteToJsonAndRead() throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter stringWriter = new StringWriter();
        MetaCodecProvider codecProvider = MetaCodecs.builder()
                .add(new StandardCodecModule())
                .add(new MetaDocumentCodec.Provider())
                .build();
        MetaCodec<MetaDocument> codec = codecProvider.resolve(MetaDocument.class);
        MetaContext context = MetaContexts.create(codecProvider);
        MetaDocument doc = MetaDocuments.create(context)
                .set("id", 1)
                .set("name", "Document 1")
                .set("nestedDoc", MetaDocument.create()
                        .set("nestedId", 2)
                        .set("nestedName", "Document 1.2"));
        try (JsonGenerator generator = jsonFactory.createGenerator(stringWriter)) {
            MetaWriter metaWriter = JacksonAdapter.writer(generator);
            codec.encode(context.ofWriter(metaWriter), doc);
        }

        String json = stringWriter.toString();
        Assert.assertEquals("{\"name\":\"Document 1\",\"id\":1,\"nestedDoc\":{\"nestedId\":2,\"nestedName\":\"Document 1.2\"}}", json);
        try (JsonParser parser = jsonFactory.createParser(json)) {
            MetaReader reader = JacksonAdapter.reader(parser);
            MetaDocument doc2 = codec.decode(context.ofReader(reader));
            Assert.assertEquals(doc.toString(), doc2.toString());
        }
    }
}
