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
import ohua.util.Lazy;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sertel on 9/7/17.
 */
public abstract class Runtime {
  Runtime() {
  }

  public static Runnable prepare(Graph<Object> graph) {
    return prepare(graph, new RuntimeProcessConfiguration());
  }

  public static Runnable prepare(Graph<Object> graph, RuntimeProcessConfiguration config) {
    OhuaRuntime runtime = new OhuaRuntime();

    Arrays.stream(graph.operators).forEach((Util.ThrowingConsumer<Operator>) op -> runtime.createOperator(op.type, op.id));
    Arrays.stream(graph.arcs)
            .filter(arc -> arc.source instanceof Source.Local)
            .forEach(arc -> runtime.registerDependency(
                    ((Source.Local) arc.source).target.operator,
                    ((Source.Local) arc.source).target.index,
                    arc.target.operator,
                    arc.target.index));

    Map<Integer, List<Arc>> envAssoc =
        Arrays.stream(graph.arcs)
              .filter(arc -> arc.source instanceof Source.Env)
              .collect(Collectors.groupingBy(
                  arc -> arc.target.operator
              ));
    envAssoc.values().forEach(
        arcs -> arcs.sort((a, b) -> a.target.index - b.target.index ));

    return (Util.ThrowingRunnable) () -> {
        envAssoc.forEach((Util.ThrowingBiConsumer<Integer, List<Arc>>) (Integer op, List<Arc> arcs) ->
            runtime.setArguments(
                op,
                arcs.stream()
                    .map(arc ->
                        new Tuple<>(arc.target.index, ((Source.Env<Object>) arc.source).hostExpr))
                    .toArray(Tuple[]::new))
        );
        runtime.execute(config);
    };
  }

}
