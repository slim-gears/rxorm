package com.slimgears.rxrepo.encoding;

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

    interface Reader extends MetaContext {
        MetaReader reader();
    }

    interface Writer extends MetaContext {
        MetaWriter writer();
    }
}
