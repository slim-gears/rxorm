package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;
import com.slimgears.util.autovalue.annotations.UseJacksonAnnotator;

@AutoValuePrototype
@UseBuilderExtension
@UseCopyAnnotator
@UseJacksonAnnotator
@UseAutoValueAnnotator
public @interface PrototypeWithBuilder {
}
