/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public abstract class Source {
    Source() {}

    public static final class Local extends Source {
        public final Target target;

        public Local(final Target target) {
            this.target = target;
        }
    }

    public static final class Env extends Source {
        public final Object hostExpr;

        public Env(final Object hostExpr) {
            this.hostExpr = hostExpr;
        }
    }
}
