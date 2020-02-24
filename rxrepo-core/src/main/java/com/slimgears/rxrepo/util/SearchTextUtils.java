package com.slimgears.rxrepo.util;

import com.google.common.base.Strings;

public class SearchTextUtils {
    public static String searchTextToRegex(String searchExpr) {
//        searchExpr = searchExpr
//                .replaceAll("[{}()|$.]", "?")
//                .replace("")
//                .replace("[", "?")
//                .replace("\\", "?")
//                .replace("]", "?");
//
//        if (Strings.isNullOrEmpty(searchExpr)) {
//            return "";
//        }
//
//        return ".*" + String.join("*", searchExpr.split("\\s"))
//                .replace("*", ".*")
//                .replace("?", ".") + ".*";
        return ".*" + searchExpr
                .replaceAll("([.$(){}|\\\\?*+])", "\\\\$1")
                .replace("[", "\\[")
                .replace("]", "\\]")
                + ".*";
    }
}
