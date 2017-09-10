/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public abstract class Source<T> {
    Source() {}

    public static final class Local extends Source {
        public final Target target;

        public Local(final Target target) {
            this.target = target;
        }
    }

    public static final class Env<T> extends Source<T> {
        public final T hostExpr;

        public Env(final T hostExpr) {
            this.hostExpr = hostExpr;
        }
    }
}
