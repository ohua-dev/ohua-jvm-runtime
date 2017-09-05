/*
 * ohua : Box.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by justusadam on 25/01/2017.
 */
public final class ImmutableBox<T> implements Box<T> {
    private Supplier<T> getter = () -> {
        throw new IllegalStateException("Uninitialised box");
    };
    private Consumer<T> setter = v -> {
        this.setter = unused -> {
            throw new IllegalStateException("Setting immutable box twice");
        };
        this.getter = () -> v;
    };

    private ImmutableBox() {
    }

    public static <T1> ImmutableBox<T1> empty() {
        return new ImmutableBox<>();
    }

    public static <T1> ImmutableBox<T1> with(T1 v) {
        ImmutableBox<T1> b = new ImmutableBox<>();
        b.set(v);
        return b;
    }

    @Override
    public T get() {
        return getter.get();
    }

    @Override
    public void set(T v) {
        setter.accept(v);
    }
}
