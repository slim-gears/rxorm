package com.slimgears.rxrepo.query;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;

@AutoValuePrototype(value = "$1", pattern = "(.*)Model")
@UseAutoValueAnnotator
@UseBuilderExtension
public interface RepositoryConfigModel {
    int retryCount();
    int debounceTimeoutMillis();
    int retryInitialDurationMillis();
}
