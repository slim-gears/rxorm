package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.UseMetaDataExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@UseMetaDataExtension
public @interface UseExpressions {
}
