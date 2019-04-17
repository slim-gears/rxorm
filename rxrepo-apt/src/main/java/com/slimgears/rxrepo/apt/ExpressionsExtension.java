package com.slimgears.rxrepo.apt;

import com.google.auto.service.AutoService;
import com.slimgears.util.autovalue.apt.Context;
import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.autovalue.apt.extensions.Extension;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import java.util.Optional;

@AutoService(Extension.class)
@SupportedAnnotationTypes("com.slimgears.rxrepo.annotations.UseExpressions")
public class ExpressionsExtension implements Extension {
    public static class ExpressionUtils {
        public boolean hasOwnExpressions(PropertyInfo property) {
            return ExtensionUtils.hasInnerClass(property, "Expressions");
        }
    }

    @Override
    public String generateClassBody(Context context) {
        return context
                .evaluatorForResource("expressions-body.java.vm")
                .variable("expressionUtils", new ExpressionUtils())
                .evaluate();
    }
}
