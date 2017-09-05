/*
 * ohua : MutableBox.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

/**
 * Created by justusadam on 25/01/2017.
 */
public final class MutableBox<T> implements Box<T> {
    private T v;

    private MutableBox() {
    }

    private MutableBox(T v) {
        this.v = v;
    }

    public static <T1> MutableBox<T1> empty() {
        return new MutableBox<T1>();
    }

    public static <T1> MutableBox<T1> with(T1 v) {
        return new MutableBox<T1>(v);
    }

    @Override
    public T get() {
        return v;
    }

    @Override
    public void set(T v) {
        this.v = v;
    }

}
