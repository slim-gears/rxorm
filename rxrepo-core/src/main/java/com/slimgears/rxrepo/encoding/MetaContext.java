package com.slimgears.rxrepo.encoding;

import com.slimgears.util.reflect.TypeToken;

import javax.annotation.Nonnull;
import java.util.Objects;

public interface MetaContext {
    MetaClassFieldMapper fieldMapper();
    MetaCodecProvider codecProvider();

    default Reader ofReader(MetaReader reader) {
        return new Reader() {
            @Override
            public MetaReader reader() {
                return reader;
            }

            @Override
            public MetaClassFieldMapper fieldMapper() {
                return MetaContext.this.fieldMapper();
            }

            @Override
            public MetaCodecProvider codecProvider() {
                return MetaContext.this.codecProvider();
            }
        };
    }

    default Writer ofWriter(MetaWriter writer) {
        return new Writer() {
            @Override
            public MetaWriter writer() {
                return writer;
            }

            @Override
            public MetaClassFieldMapper fieldMapper() {
                return MetaContext.this.fieldMapper();
            }

            @Override
            public MetaCodecProvider codecProvider() {
                return MetaContext.this.codecProvider();
            }
        };
    }

    @SuppressWarnings("unchecked")
    default <T> void write(MetaWriter writer, @Nonnull T value) {
        MetaCodec<T> codec = codecProvider().resolve((Class<T>)Objects.requireNonNull(value).getClass());
        codec.encode(ofWriter(writer), value);
    }

    default <T> T read(MetaReader reader, TypeToken<T> type) {
        return codecProvider().resolve(type).decode(ofReader(reader));
    }

    default <T> T read(MetaReader reader, Class<? extends T> type) {
        return codecProvider().resolve(type).decode(ofReader(reader));
    }

    interface Reader extends MetaContext {
        MetaReader reader();
    }

    interface Writer extends MetaContext {
        MetaWriter writer();
    }
}
