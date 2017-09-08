/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua;

import ohua.graph.Arc;
import ohua.graph.Graph;
import ohua.graph.Operator;
import ohua.graph.Source;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.lang.OhuaRuntime;
import ohua.util.Tuple;
import ohua.util.Util;

import java.util.Arrays;

/**
 * Created by sertel on 9/7/17.
 */
public abstract class Runtime {
  Runtime() {
  }

  public static Runnable prepare(Graph graph) {
    return prepare(graph, new RuntimeProcessConfiguration());
  }

  public static Runnable prepare(Graph graph, RuntimeProcessConfiguration config) {
    OhuaRuntime runtime = new OhuaRuntime();

    Arrays.stream(graph.operators).forEach((Util.ThrowingConsumer<Operator>) op -> runtime.createOperator(op.type, op.id));
    Arrays.stream(graph.arcs)
            .filter(arc -> arc.source instanceof Source.Local)
            .forEach(arc -> runtime.registerDependency(
                    ((Source.Local) arc.source).target.operator,
                    ((Source.Local) arc.source).target.index,
                    arc.target.operator,
                    arc.target.index));
    Arrays.stream(graph.arcs)
            .filter(arc -> arc.source instanceof Source.Env)
            .forEach((Util.ThrowingConsumer<Arc>)
                    arc -> runtime.setArguments(
                            arc.target.operator,
                            new Tuple[]{new Tuple(arc.target.index, ((Source.Env) arc.source).hostExpr)}));

    return (Util.ThrowingRunnable) () -> runtime.execute(config);
  }

}
