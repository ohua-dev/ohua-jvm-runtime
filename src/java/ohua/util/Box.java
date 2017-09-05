/*
 * ohua : Box.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

/**
 * Created by justusadam on 25/01/2017.
 */
public interface Box<T> {
    T get();

    void set(T v);
}
