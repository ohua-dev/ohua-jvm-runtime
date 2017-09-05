/*
 * ohua : TypeError.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

/**
 * Created by justusadam on 07/02/2017.
 */
public class TypeError extends RuntimeException {
    private TypeError(String message) {
        super(message);
    }

    private TypeError() {
        super();
    }

    private TypeError(String message, Throwable cause) {
        super(message, cause);
    }

    private TypeError(Throwable cause) {
        super(cause);
    }

    protected TypeError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TypeError(Class<?> c1, Class<?> c2) {
        super(formatMessage(c1.toString(), c2.toString()));
    }

    public TypeError(String t1, String t2) {
        super(formatMessage(t1, t2));
    }

    private static String formatMessage(String t1, String t2) {
        return "Type error, expected " + t1 + " got " + t2;
    }
}
