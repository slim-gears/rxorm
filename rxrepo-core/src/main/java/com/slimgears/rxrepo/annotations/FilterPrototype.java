package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;
import com.slimgears.util.autovalue.annotations.UseJacksonAnnotator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@UseBuilderExtension
@UseJacksonAnnotator
@UseAutoValueAnnotator
@UseCopyAnnotator
@AutoValuePrototype
public @interface FilterPrototype {
}
