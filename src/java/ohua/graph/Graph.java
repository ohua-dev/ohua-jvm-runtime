/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public final class Graph {
    public final Operator[] operators;
    public final Arc[] arcs;

    public Graph(final Operator[] operators, final Arc[] arcs) {
        this.operators = operators;
        this.arcs = arcs;
    }
}
