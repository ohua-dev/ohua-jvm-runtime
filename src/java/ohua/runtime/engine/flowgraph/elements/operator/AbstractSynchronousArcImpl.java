/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.AbstractArcQueue;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

import java.util.List;

/**
 * Created by sertel on 4/5/16.
 */
public abstract class AbstractSynchronousArcImpl extends AbstractNotificationBasedArcImpl {
  protected Object _packet = null;

  public AbstractSynchronousArcImpl(Arc arc) {
    super(arc);
  }

  protected final boolean fusedDownstreamExecution(NotificationBasedOperatorRuntime op) {
    op.resetQuanta();
    op.executeOperator().run(op.getOp().getOperatorAlgorithm());
    OperatorStateMachine.BackoffReason r = op.getBackOffReason();
    switch(r){
      case FULL_ARC:
        // do the job of the operator state machine and reset here!
        op.resetMonitoringState();
        return true;
      default: return false;
    }
  }

  protected final void fusedUpstreamExecution(NotificationBasedOperatorRuntime op) {
    // it is unclear whether this operator is really allowed to run because an operator is only allowed to run when it is not blocking
    // -> all ports where unblocked by the activation marker.
    if(op.isOperatorBlocking()){
      op.skimMetaData(false);
      if(op.isOperatorBlocking()){
        return;
      }
    }

    // FIXME this should only be done when there is a NULL_DEQUE
    op.resetQuanta();
    op.executeOperator().run(op.getOp().getOperatorAlgorithm());
  }

  @Override
  public void enqueueBatch(List<? extends IStreamPacket> batch) {
    Assertion.invariant(batch.size() < 2);
    if (!batch.isEmpty()) {
      _packet = batch.get(0);
    }
  }

  @Override
  protected void enqueueMetaData(IStreamPacket metaDataPacket) {
    _packet = metaDataPacket;
  }

  @Override
  public int getLoadEstimate() {
    return _packet != null ? 1
            : 0;
  }

  @Override
  public void sweep() {
    _packet = null;
  }

  @Override
  public Maybe<Object> peek() {
    return super.get(_packet);
  }

  @Override
  public AbstractArcQueue.DataQueueIterator getDataIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isArcEmpty() {
    return _packet == null;
  }

  @Override
  public void transferTo(AbstractArcImpl arcImpl) {
    // this arc is stateless, so there is nothing to transfer
  }

  public String toString() {
    return "sync-arc";
  }

}
