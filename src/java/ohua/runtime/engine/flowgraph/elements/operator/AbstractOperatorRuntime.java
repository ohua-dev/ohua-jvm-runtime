/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.*;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;
import ohua.runtime.engine.operators.system.ProcessControlOperator;

/**
 * Created by sertel on 1/26/17.
 */
public abstract class AbstractOperatorRuntime {

  public static final int DEFAULT_SCHEDULING_PRIORITY = -1;
  protected OperatorCore _op;
  private OperatorExecution _operatorExecutor = new OperatorExecution();
  private boolean _isActive = true;
  private AbstractOperatorStateMachine.OperatorState _state = AbstractOperatorStateMachine.OperatorState.INIT;
  private int _graphPriority = DEFAULT_SCHEDULING_PRIORITY;
  private boolean _tearDownNow = false;
  private SystemPhaseType _systemPhase = null;

  public AbstractOperatorRuntime(OperatorCore op, RuntimeProcessConfiguration config) {
    _op = op;
  }

  public final OperatorCore getOp() {
    return _op;
  }

  public void activate() {
    _isActive = true;
  }

  protected void deactivate() {
    _isActive = false;
  }

  protected void setActive(boolean isActive){ _isActive = isActive; }

  public boolean isActive() {
    return _isActive;
  }

  public int getGraphPriority() {
    return _graphPriority;
  }

  public void setGraphPriority(int priority) {
    _graphPriority = priority;
  }

  /**
   * Pushes the data packet into all outgoing arcs of the output port identified by the given
   * index.
   *
   * @param data
   * @return
   */
  public boolean pushData(OutputPort outPort, Object data) {
    Assertion.invariant(_op.getOutputPorts().size() > 0);
    return outPort.sendDataPacketNew(data);
  }

  public boolean broadcast(IStreamPacket data) {
    return _broadcast(data, true).handlePushDataReturnValue();
  }

  public boolean broadcast(IStreamPacket data, boolean broadcastToMetaPorts) {
    return _broadcast(data, broadcastToMetaPorts).handlePushDataReturnValue();
  }

  private PushDataReturnValue _broadcast(IStreamPacket data, boolean broadcastToMetaPorts) {
    PushDataReturnValue returnValue = PushDataReturnValue.BOUNDARY_NOT_YET_REACHED;

    for (OutputPort outPort : _op.getOutputPorts()) {
      if (outPort.isMetaPort() && !broadcastToMetaPorts) {
        continue;
      } else {
        // the packet decides whether it can become shared memory or not
        data = data.deepCopy();

        boolean returnStatus = outPort.sendDataPacketNew(data);

        PushDataReturnValue currentReturnValue = handlePortReturnStatus(returnStatus);
        returnValue = returnValue.apply(currentReturnValue);
      }
    }

    return returnValue;
  }

  public void enqueueMetaData(OutputPort outPort, IMetaDataPacket packet) {
    outPort.enqueueMetaData(packet);
  }

  public Maybe<Object> pollData(InputPort inputPort) {
    Maybe<Object> dequeuedDataPacket = inputPort.dequeueDataPacket();
    Assertion.invariant(!(dequeuedDataPacket.get() instanceof IMetaDataPacket));
    return dequeuedDataPacket;
  }

  protected void finishOutputPorts() {
    // send an end-of-computation handler downstream
    _op.getOutputPorts().forEach(OutputPort::finish);
  }

  protected void closeOutputPorts() {
    // send an end-of-computation handler downstream
    _op.getOutputPorts().forEach(OutputPort::close);
  }

  public void register(OperatorEvents opEvent, IOperatorEventListener opEventListener) {
    opEvent.register(_op.getId(), opEventListener);
  }

  protected void raiseOperatorEvent(OperatorEvents event) {
    event.raise(_op.getId());
  }

  public AbstractOperatorStateMachine.OperatorState getOperatorState() {
    return _state;
  }

  /**
   * This function should be used very carefully because it interferes with the internal state
   * machine! (Used only on restart to set the state saved in the checkpoint!)
   *
   * @param state
   */
  protected void setOperatorState(AbstractOperatorStateMachine.OperatorState state) {
    _state = state;
  }

  public final void prepareAndEnterComputationState() {
    // TRANSITION: INIT -> WAITING_FOR_COMPUTATION (user ops)/WAITING_FOR_DATA (system ops)
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.INIT);
    runOperatorStep();
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION);
  }

  protected final boolean isTearDown() {
    return _tearDownNow;
  }

  public void tearDownAndFinishComputation() {
    // FIXME redundant flag! use the process state!
    _tearDownNow = true;

    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION ||
            // I could not get the old runtime implementation to transition the ProcessControl into the above state.
            (this.getOp().getOperatorAlgorithm() instanceof ProcessControlOperator &&
            getOperatorState() == AbstractOperatorStateMachine.OperatorState.WAITING), () -> getOp().getOperatorName() + ": " +getOperatorState().name());

    // need activation and EOS markers to get into the proper state
    _op.getInputPorts().stream()
            .map(InputPort::getIncomingArc)
            .forEach(a -> {
              a.enqueue(PacketFactory.createActivationMarkerPacket(SystemPhaseType.TEARDOWN));
              a.enqueue(PacketFactory.createEndSignalPacket(0, SystemPhaseType.TEARDOWN));
            });
    // TODO this is the only switch needed to bring the operator into the executing meta data state. remove the markers!
    setProcessState(SystemPhaseType.TEARDOWN);

    runOperatorStep();
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.EXECUTING_META_DATA);
    runOperatorStep();
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.EXECUTING_EPILOGUE);
    runOperatorStep();
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.CLEAN_UP);
    runOperatorStep();
    Assertion.invariant(getOperatorState() == AbstractOperatorStateMachine.OperatorState.DONE);

    // delete the packets that have been enqueued
    _op.getGraphNodeOutputConnections().forEach(Arc::sweep);
  }

  public final SystemPhaseType getProcessState() {
    return _systemPhase;
  }

  public final void setProcessState(SystemPhaseType phase) {
    _systemPhase = phase;
  }

  abstract public void runOperatorStep();

  public OperatorExecution executeOperator() {
    return _operatorExecutor;
  }

  abstract protected PushDataReturnValue handlePortReturnStatus(boolean returnStatus);

  protected void handleMetaData() {
    skimMetaData(false);
  }

  protected int skimMetaData(InputPort inPort) {
    return inPort.skimMetaData();
  }

  /**
   * Only skim for meta data packets that are independent of the state of this operator. (Later
   * on we will introduce a better operator state management and then we can get rid of this
   * function again.)
   */
  protected int skimSpecialMetaData(InputPort inPort) {
    return inPort.skimSpecialMetaData();
  }

  protected void skimMetaData(boolean special) {
    for (InputPort inPort : _op.getInputPorts()) {
      if (special) {
        skimSpecialMetaData(inPort);
      } else {
        skimMetaData(inPort);
      }
    }
  }

  public boolean isInputComplete() {
    for(InputPort inPort : _op.getInputPorts()) {
      if(!inPort.hasSeenLastPacket()) return false;
    }
    return true;
  }

  public boolean isComputationComplete() {
    return isInputComplete() &&
            (_op.getOperatorAlgorithm() instanceof IDone ?
                    ((IDone) _op.getOperatorAlgorithm()).isComputationComplete() :
                    hasFinishedComputation());
  }

  protected boolean hasFinishedComputation(){
    return true;
  }
      @Override
  public String toString() {
    return _op.toString();
  }

  // these are the return information that the caller (=operator) receives when pushing data
  // into an output port. here we rely on the fact that every operator handles these information
  // properly (gives back control to the scheduler when it receives the information to do so).
  public enum PushDataReturnValue {
    BOUNDARY_REACHED(true),
    BOUNDARY_NOT_YET_REACHED(false),
    RETURN_CONTROL(true);

    boolean _yieldControl;

    PushDataReturnValue(boolean yieldControl) {
      _yieldControl = yieldControl;
    }

    public boolean handlePushDataReturnValue() {
      return _yieldControl;
    }

    PushDataReturnValue apply(PushDataReturnValue other) {
      if (this == RETURN_CONTROL) return this;
      else {
        switch (other) {
          case RETURN_CONTROL:
            return PushDataReturnValue.RETURN_CONTROL;
          case BOUNDARY_REACHED:
            return PushDataReturnValue.BOUNDARY_REACHED;
          case BOUNDARY_NOT_YET_REACHED:
            // do nothing here!
            return this;
        }
        throw new RuntimeException("impossible");
      }
    }
  }

}