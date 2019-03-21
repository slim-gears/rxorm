package com.slimgears.rxrepo.annotations;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
@AutoValuePrototype.Extension({
        "com.slimgears.rxrepo.apt.ExpressionsExtension",
        "com.slimgears.util.autovalue.apt.MetaDataExtension"})
public @interface AutoValueExpressions {
}
