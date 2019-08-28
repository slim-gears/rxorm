package com.slimgears.rxrepo.util;

import com.google.common.base.Strings;

class SearchTextUtils {
    static String searchTextToRegex(String searchExpr) {
        if (Strings.isNullOrEmpty(searchExpr)) {
            return "";
        }

        String searchText = String.join("*", searchExpr.split("\\s"));
        return searchText
                .replaceAll("([.$(){}|\\[\\]])", "\\$1")
                .replace("$", "\\$")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("?", ".")
                .replace("*", ".*");
    }
}
