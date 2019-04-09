package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

@AutoValuePrototype
@UseBuilderExtension
@UseCopyAnnotator
@UseAutoValueAnnotator
public @interface PrototypeWithBuilder {
}
