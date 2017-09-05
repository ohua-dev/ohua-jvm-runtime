/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;

@Deprecated //FIXME -> we have another work item on a new concept for operator fusion done better.
public class SynchronousArcImpl extends AbstractSynchronousArcImpl
{

  public SynchronousArcImpl(Arc arc)
  {
    super(arc);
  }

  /**
   * Only a single dequeue is always possible.
   */
  @Override
  public Maybe<Object> getData()
  {
    Object packet = _packet;
    _packet = null;
    if(packet == null)
    {
      super.activateUpstream();
      return super.get();
    }else {
      return super.get(packet);
    }
  }
  
  /**
   * There is no limitation on the number of enqueue operations from the perspective of the arc
   * implementation (for now).</br> Whenever this function is being invoked it directly calls
   * the process routine of the downstream operator.
   */
  @Override
  protected boolean enqueue(Object packet)
  {
    throw new UnsupportedOperationException();
    // FIXME refactoring
//    _packet = packet;
//    OperatorCore target = _arc.getTarget();
//    // we have to make sure when running this operator that it will never fail in retrieving the packet!
//    target.resetQuanta();
//    target.executeOperator().run(_arc.getTarget().getOperatorAlgorithm());
//    // the operators must have been set into computation state manually because a marker-based activation requires to run the operator state machine of this op!
//    assert target.getOperatorState() == OperatorStateMachine.OperatorState.WAITING_FOR_DATA
//            || target.getOperatorState() == OperatorStateMachine.OperatorState.CAN_PRODUCE_MORE_DATA
//            : target.getOperatorName() + ":" + target.getOperatorState();
//    super.activateDownstream();
//    return true;
  }
}
