package com.slimgears.rxrepo.encoding.codecs;

import com.slimgears.rxrepo.encoding.MetaCodec;
import com.slimgears.rxrepo.encoding.MetaContext;

public class ObjectCodec implements MetaCodec<Object> {
    @Override
    public void encode(MetaContext.Writer context, Object value) {
    }

    @Override
    public Object decode(MetaContext.Reader context) {
        return null;
    }
}
