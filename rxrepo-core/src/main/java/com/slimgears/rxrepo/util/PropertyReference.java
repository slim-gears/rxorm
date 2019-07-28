package com.slimgears.rxrepo.util;

import com.slimgears.util.autovalue.annotations.PropertyMeta;

public interface PropertyReference {
    String referencePath();
    PropertyMeta<?, ?> property();
}
