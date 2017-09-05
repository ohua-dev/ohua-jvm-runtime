/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.ArcID;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

/**
 * Source work resembles data that is present and needs to be consumed. Source work associates
 * produced data with an operator (via an arc reference) responsible for handling it.
 * @author sertel
 * 
 */
public class SourceWork implements Work
{
  private Arc _arc = null;
  private WorkChunk _chunk = null;
  
  public SourceWork(Arc arc, WorkChunk chunk) {
    _arc = arc;
    _chunk = chunk;
  }
  
  public OperatorCore activate(){
    Assertion.invariant(_chunk.size() > 0);
    TaskBasedAsynchronousArc arcImpl = (TaskBasedAsynchronousArc) _arc.getImpl();
    arcImpl.assignWork(_chunk);
    return _arc.getTarget();
  }

  @Override public Work reportIncompleteWork() {
    // clear the arc again because work either is in the work queue or at an arc, not both.
    ((TaskBasedAsynchronousArc) _arc.getImpl()).releaseUnfinishedWork();
    return _chunk.size() > 0 ? this : null;
  }

  @Override public OperatorID getReference() {
    return _arc.getTarget().getId();
  }

  @Override public ArcID getLocationReference() {
    return _arc.getArcId();
  }

  @Override public int size() {
    return _chunk.size();
  }

  @Override public int limit() {
    return _arc.getArcBoundary();
  }

}
