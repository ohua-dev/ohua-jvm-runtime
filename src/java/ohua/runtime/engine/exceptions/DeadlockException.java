/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.exceptions;

import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateMachine;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.sections.SectionScheduler;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sertel on 8/22/16.
 */
public class DeadlockException extends RuntimeException {
  /**
   * The assumption here: the notification algorithm in the arcs works properly.
   */
  private static String MSG = "A deadlock in the execution of Ohua is the result of a bug in an operator implementation:\n" +
          "When an operator receives data among its incoming arcs it gets scheduled and eventually executed. " +
          "A deadlock occurs when the operator performs no actions among its ports. " +
          "As a result, no new notifications are created, i.e. no new operators are being scheduled, and the execution deadlocks.\n" +
          "Here is an analysis that should help you debug the problem:";

  public SectionGraph _graph;
  public List<SectionScheduler.FinishedSectionExecution> _lastScheduledTasks;
  public Map<OperatorCore, ? extends AbstractOperatorRuntime> _runtimes;

  public DeadlockException(SectionGraph graph, List<SectionScheduler.FinishedSectionExecution> lastScheduledTasks, Map<OperatorCore, ? extends AbstractOperatorRuntime> runtimes) {
    super();
    _graph = graph;
    _lastScheduledTasks = lastScheduledTasks;
    _runtimes = runtimes;
  }

  public String getMessage() {
    return MSG +
            "\nEXECUTION:\n" + executionAnalysis() +
            "\nGRAPH:\n" + graphAnalysis();
  }

  private String executionAnalysis(){
    StringBuilder strBuf = new StringBuilder();
    for(SectionScheduler.FinishedSectionExecution lastSectionTask : _lastScheduledTasks) {
      strBuf.append("------------------------------------------------------------------------------------------\n");
      strBuf.append("SectionScheduler.section: ").append(lastSectionTask._section).append("\n");
      strBuf.append("SectionScheduler.section.executed: ").append(lastSectionTask._executed).append("\n");
      strBuf.append("SectionScheduler.section.result: ").append(lastSectionTask._result).append("\n");
      strBuf.append("SectionScheduler.section.activations: ").append(lastSectionTask._activations).append("\n");
      strBuf.append("SectionScheduler.section.start: ").append(lastSectionTask._start).append("\n");
      strBuf.append("SectionScheduler.section.finished: ").append(lastSectionTask._finished).append("\n");
      strBuf.append(lastSectionTask._section.deadlockAnalysis());
    }
    return strBuf.toString();
  }

  private String graphAnalysis() {
    StringBuffer strBuf = new StringBuffer().append("Arc impl used = ").append(_graph.getAllArcs().get(0).getImpl().getClass()).append("\n");

    strBuf.append("Head operators waiting for data: \n");
    strBuf.append(_runtimes.values().stream()
            .filter(op -> op.getOperatorState() != OperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION && op.getOp().getAllPreceedingGraphNodes().stream().allMatch(inner -> _runtimes.get(inner).getOperatorState() == OperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION))
            .map(AbstractOperatorRuntime::getOp)
            .map(OperatorCore::getOperatorName)
            .collect(Collectors.joining(", ")));
    strBuf.append("\n");
    strBuf.append("Arcs with data:\n");
    _graph.getAllArcs().stream()
            .filter(arc -> arc.getLoadEstimate() > 0)
            .forEach(arc ->
                    strBuf.append(arc.getSource()).append("** ").append(arc.getSourcePort()).append(" -- ").append(arc.getArcId()).append("[").append(arc.stringifyData()).append("] -- ").append(arc.getTargetPort()).append(" **>").append(arc.getTarget()).append(" ").append("[meta: load estimate = ").append(arc.getLoadEstimate()).append(" arc boundary = ").append(arc.getArcBoundary()).append(", arc type = ").append(arc.getType()).append("]\n")
            );

    strBuf.append("\n");

    return strBuf.toString();
  }
}
