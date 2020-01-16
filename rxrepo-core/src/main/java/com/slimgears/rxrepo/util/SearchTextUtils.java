package com.slimgears.rxrepo.util;

import com.google.common.base.Strings;

class SearchTextUtils {
    static String searchTextToRegex(String searchExpr) {
        if (Strings.isNullOrEmpty(searchExpr)) {
            return "";
        }

        return String.join("*", searchExpr.split("\\s"))
                .replaceAll("([.$(){}|[\\\\]])", "\\\\$1")
                .replace("$", "\\$")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("?", ".")
                .replace("*", ".*");
    }
}
