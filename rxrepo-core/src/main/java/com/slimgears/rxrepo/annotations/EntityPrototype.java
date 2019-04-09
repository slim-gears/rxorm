package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@AutoValuePrototype(pattern = "(.*)", value = "$1Entity")
@UseExpressions
@UseCopyAnnotator
public @interface EntityPrototype {
}
