package com.slimgears.rxrepo.apt;

import com.slimgears.util.autovalue.apt.Context;
import com.slimgears.util.autovalue.apt.Extension;

public class ExpressionsExtension implements Extension {
    @Override
    public String generateClassBody(Context context) {
        return context.evaluateResource("expressions-body.java.vm");
    }
}
