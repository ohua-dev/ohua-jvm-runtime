/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public final class Arc {
    public final Target target;
    public final Source source;

    public Arc(final Target target, final Source source) {
        this.target = target;
        this.source = source;
    }
}
