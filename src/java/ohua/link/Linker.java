/*
 * ohua : Linker.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.link;


import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Created by justusadam on 01/12/2016.
 */
public final class Linker {
    private static final IFn require = Clojure.var("clojure.core", "require");
    private static final IFn isBuiltin;
    private static final IFn isDefined;
    private static final IFn toJavaName;

    static {
        require.invoke(Clojure.read("ohua.link"));
        require.invoke(Clojure.read("com.ohua.clojure-backend"));
        isBuiltin = Clojure.var("ohua.link", "is-builtin?");
        isDefined = Clojure.var("ohua.link", "is-defined?");
        toJavaName = Clojure.var("com.ohua.clojure-backend/to-java-name");
    }

    private Linker() {
    }

    public static String convertFunctionName(String in) {
        return (String) toJavaName.invoke(in);
    }

}
