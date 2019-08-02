package com.slimgears.rxrepo.encoding;

public interface MetaCodec<T> {
    void encode(MetaContext.Writer context, T value);
    T decode(MetaContext.Reader context);
}
