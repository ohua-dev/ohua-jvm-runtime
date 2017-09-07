/*
 * ohua : JavaBackendProvider.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.link;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.lang.reflect.Method;

/**
 * Created by justusadam on 01/12/2016.
 */
@Deprecated // I believe that this is a ClojureSFNProvider only
public final class JavaBackendProvider {
    private static final IFn require = Clojure.var("clojure.core", "require");
    private static final IFn getBackend_;
    private static final IFn registerFunction_;
    private static final IFn clearCache_;
    private static final IFn loadCoreOperators_;

    static {
        require.invoke(Clojure.read("ohua.link"));
        require.invoke(Clojure.read("com.ohua.clojure-backend"));
        getBackend_ = Clojure.var("ohua.link", "get-backend");
        registerFunction_ = Clojure.var("com.ohua.clojure-backend", "register-function!");
        clearCache_ = Clojure.var("com.ohua.clojure-backend", "clear-cache!");
        loadCoreOperators_ = Clojure.var("com.ohua.clojure-backend", "load-core-operators");
    }

    private JavaBackendProvider() {
    }

    public static SFNBackend getBackend() {
        return (SFNBackend) getBackend_.invoke();
    }

    public static void registerFunction(String ns, String name, Method handle) {
        registerFunction_.invoke(ns, name, handle);
    }

    public static void clearCache() {
        clearCache_.invoke();
    }

    public static void loadCoreOperators() {
        loadCoreOperators_.invoke();
    }
}
