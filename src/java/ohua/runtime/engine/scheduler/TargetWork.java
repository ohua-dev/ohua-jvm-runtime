/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.ArcID;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

/**
 * Target work resembles either internal operator state or data coming from external resources.
 * Target work is only present when all incoming arcs of an operator have finished execution.
 * 
 * @author sertel
 * 
 */
public class TargetWork implements Work
{
  private OperatorCore _op = null;
  
  protected TargetWork(OperatorCore op) {
    _op = op;
  }
  
  @Override public OperatorCore activate() {
    return _op;
  }
  
  /**
   * Work for an input complete operator needs to be resubmitted by the guy (TaskExecutor) that
   * decides whether it is done or no.
   */
  @Override public Work reportIncompleteWork() {
    return null;
  }
  
  @Override public OperatorID getReference() {
    return _op.getId();
  }

  @Override public ArcID getLocationReference() {
    assert _op.getNumGraphNodeInputs() > 0;
    return _op.getGraphNodeInputConnections().get(0).getArcId();
  }
  
  @Override public int size() {
    return 1;
  }
  
  @Override public int limit() {
    return 1;
  }

}
