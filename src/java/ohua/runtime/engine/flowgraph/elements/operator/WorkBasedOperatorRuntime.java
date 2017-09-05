/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.scheduler.WorkBasedAsynchronousArc;
import ohua.runtime.engine.scheduler.WorkChunk;

import java.util.List;

/**
 * Created by sertel on 1/28/17.
 */
public class WorkBasedOperatorRuntime extends AbstractOperatorRuntime {

  private WorkBasedOperatorStateMachine _stateMachine;

  public WorkBasedOperatorRuntime(OperatorCore op, RuntimeProcessConfiguration config) {
    super(op, config);
    _stateMachine = new WorkBasedOperatorStateMachine(this);
  }

  protected PushDataReturnValue handlePortReturnStatus(boolean returnStatus) {
    return !returnStatus ? PushDataReturnValue.BOUNDARY_REACHED : PushDataReturnValue.BOUNDARY_NOT_YET_REACHED;
  }

  public boolean pushData(OutputPort outPort, Object data) {
    boolean canEnqueueMore = super.pushData(outPort, data);
    return !canEnqueueMore;// so don't return control to the scheduler (skip this port return status code!)
  }

  @Override
  public void runOperatorStep() {
    _stateMachine.runTransition();
  }

  protected boolean hasFinishedComputation() {
    // this is my last try. when this does not work then just change the operator API to always implement IDone.
    return getOp().getOutputPorts().stream()
            .map(OutputPort::getOutgoingArcs)
            .flatMap(List::stream)
            // there would have been enough space but the op decided to back out -> done.
            .allMatch(a -> a.getArcBoundary() > a.getLoadEstimate()) ||
            getOp().getOutputPorts().stream()
                    .map(OutputPort::getOutgoingArcs)
                    .flatMap(List::stream)
                    .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
                    .allMatch(a -> {
                      WorkChunk w = a.releaseResultChunk();
                      a.assignResultChunk(w);
                      // note this: the OutputPortController API does not provide a method to check whether the outgoing arcs are
                      // full. the op has to enqueue at least one packet in order to get this info. so when it did not do that at all,
                      // then processing is defined to be done.
                      return w.size() == w.getMemoizedSize();
                    });
  }

  public void tearDownAndFinishComputation() {
    _op.getGraphNodeInputConnections()
            .stream()
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> {
              // shared work chunk for marker propagation
              WorkChunk w = new WorkChunk();
              wa.assignWork(w);
              wa.assignResultChunk(w);
            });

    _op.getGraphNodeOutputConnections()
            .stream()
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> wa.assignResultChunk(new WorkChunk()));

    super.tearDownAndFinishComputation();

    _op.getGraphNodeInputConnections()
            .stream()
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> {
              wa.releaseWork();
              wa.releaseResultChunk();
            });
    _op.getGraphNodeOutputConnections()
            .stream()
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(WorkBasedAsynchronousArc::releaseResultChunk);
  }
}
