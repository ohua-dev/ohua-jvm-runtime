/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.flowgraph.elements.operator.WorkBasedOperatorRuntime;
import ohua.runtime.lang.PreparedRuntimeConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sertel on 9/7/17.
 */
public class TaskSchedulerDebugger implements PreparedRuntimeConfiguration.ITaskSchedulerDebugger {
    private static final IFn require = Clojure.var("clojure.core/require");
    private static final IFn spit = Clojure.var("clojure.core", "spit");
    private static final IFn renderfn;

    static {
      require.invoke(Clojure.read("[ohua.util.visual]"));
      renderfn = Clojure.var("ohua.util.visual", "render-op-graph");
    }

  public void dumpGraph(Set graph, int chosen, String filename) {
      if (graph == null) throw new RuntimeException("No graph!");
      renderfn.invoke(graph, chosen, filename);
  }

  public void dumpGraph(Set graph, String filename) {
      if (graph == null) throw new RuntimeException("No graph!");
      renderfn.invoke(graph, filename);
  }

  public void dumpReasoning(final int desiredWorkSize, Map<WorkBasedOperatorRuntime, WorkBasedTaskScheduler.Either> possibleWork, String filename) {
      spit.invoke(
              filename,
              possibleWork.keySet().stream().map(rt ->
                      rt.getOp().getId().getIDInt()
                              + "," + rt.getGraphPriority()
                              + ",["
                              + rt.getOp().getInputPorts()
                              .stream()
                              .map(p ->
                                      "[" + p.getIncomingArc().getSource().getId().getIDInt() + "-> :" + (p.hasSeenLastPacket() || (p.getIncomingArc().getLoadEstimate() >= desiredWorkSize)) + "]")
                              .collect(Collectors.joining(",")) + "],["
                              + rt.getOp().getOutputPorts()
                              .stream()
                              .map(OutputPort::getOutgoingArcs)
                              .flatMap(List::stream)
                              .map(a ->
                                      " ->" + a.getTarget().getId().getIDInt() + " : " + (a.getRemainingCapacityEstimate() >= desiredWorkSize))
                              .collect(Collectors.joining(","))
                              + "]"
              ).collect(Collectors.joining("\n")));
  }


}
