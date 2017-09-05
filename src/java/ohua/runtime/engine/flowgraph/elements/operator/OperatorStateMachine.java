/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.InvariantBroken;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractPort.PortState;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class OperatorStateMachine extends AbstractOperatorStateMachine<NotificationBasedOperatorRuntime> {

  protected boolean _isBlocked = false;
  protected boolean _noMoreDownstreamActivations = true;
  protected String _nextActivation = null;
  // FIXME these should be owned by the scheduler, not individually by each operator state machine!
  private Set<InputPort> _upstreamOpsToActivate = new LinkedHashSet<>();
  private Set<OperatorCore> _downstreamOpsToActivate = new LinkedHashSet<>();
  private boolean _executedOperator = false;
  private BackoffReason _backoffReason = BackoffReason.NULL_DEQUEUE;

  protected OperatorStateMachine(NotificationBasedOperatorRuntime runtime) {
    super(runtime);
  }

  protected void cleanUp(){
    super.cleanUp();
    activateDownstreamOperators();
  }

  protected Transition executingEpilogue(){
    // this line is a good help for debugging scheduling problems
//    System.out.printf("Thread: %30s | Op %40s >> Priority: %3d | Used quanta: %10d | backoff reason: %15s | executed: %5s\n", Thread.currentThread().getName(), _operatorRuntime.getOp().getOperatorName(), _operatorRuntime.getGraphPriority(), _operatorRuntime.usedQuanta(), _backoffReason, _executedOperator);

    Transition t;
    // find the resulting state
    if (isTearDown()) {
      // This means we really want to tear down the flow.
      _operatorRuntime.activate();
      t = Transition.FINISH_ALL_PROCESSING;
    } else if (_operatorRuntime.isQuantaExhausted()) {
            /*
             * Deactivate this operator but put it back into the queue! This allows other
             * operators with higher priority to run and makes sure the operator retrieves meta
             * data when it comes back in. Note though that we do not perform an activation of
             * other operators here because this is only controlled by the arc boundary concept!
             */
      _operatorRuntime.activateOperator(_operatorRuntime);
      _operatorRuntime.deactivate();
      t = Transition.EXECUTING_EPILOGUE_to_WAITING;
    } else if (isComputationComplete()) {
            /*
             * Note that we do enter only once here in the round when we retrieved the EOS.
             * Every subsequent round behaves as if we are blocked (because the output ports are
             * closed). This is ok though because we do not want to keep calling
             * finishOutputPorts().
             */

      // This is a bit weaker than tear down. It just says that we finished the computation
      // but keep the flow/process alive in order for successive computations to come.

      // if we are done then the upstream ops are also done
      _operatorRuntime.activateDownstreamNotifications(); // make sure we always catch these activations in our return status, no matter what!
      _operatorRuntime.finishOutputPorts();
      _operatorRuntime.deactivate();

      /*
       * Remember that we always submit an [EOS,Activation]-pair when we start the flow. Hence, all system ops that
       * pipe something into the graph are going into the WAITING_FOR_COMPUTATION state and would not be sensitive
       * anymore to any packets from the "outside". Therefore, the decision is to place them always in the WAITING_FOR_DATA
       * state and only define the WAITING_FOR_COMPUTATION state for user ops.
       */
//      t = _operatorRuntime.getOp().isSystemComponent() ? Transition.EXECUTING_EPILOGUE_to_WAITING :
//              Transition.WAIT_FOR_NEW_COMPUTATION;
      t = Transition.WAIT_FOR_NEW_COMPUTATION;
    } else if (_operatorRuntime.isInputComplete()
            && !_operatorRuntime.getOp().isSystemComponent()
            && _operatorRuntime.getOp().getMetaOutputPorts().size() == _operatorRuntime.getOp().getOutputPorts().size()) {
                  /*
                   * if this is the case then this op receives no more downstream activations from upstream.
                   * since it only has meta ports it only has system ops downstream which do not issue upstream activations.
                   * in order to avoid deadlocks, the operator must schedule itself again until it is done.
                   */
      _operatorRuntime.activateOperator(_operatorRuntime);
      t = Transition.EXECUTING_EPILOGUE_to_FINISHING_COMPUTATION;
    } else {
          /*
           * The normal processing state.
           */
      switch (_backoffReason) {
        case META_PRIOTITY:
          // reactivate this operator because there might still be data pending on the
          // incoming arcs that we got notified for but did not dequeue yet.
          _operatorRuntime.activateOperator(_operatorRuntime);
          //$FALL-THROUGH$
        case FULL_ARC:
//                if(_upstreamOpsToActivate.isPresent() && _downstreamOpsToActivate.isPresent()){
//                  Assertion.impossible("Operator (" + _operatorRuntime.getOperatorName() + ") backed out because it was blocked but did not activate any downstream operators. That should never happen.");
//                }
          t = Transition.EXECUTING_EPILOGUE_to_WAITING;
          break;
        case OPERATOR_DECISION:
        case NULL_DEQUEUE:
          checkQuietOperator();
          for (InputPort inPort : _operatorRuntime.getOp().getInputPorts()) {
            if (inPort.isMetaPort()) {
              _upstreamOpsToActivate.add(inPort);
            }
          }

          t = Transition.EXECUTING_EPILOGUE_to_WAITING;
          break;
        default:
          throw new RuntimeException("impossible");
      }
      _operatorRuntime.deactivate();
    }

    // filters upstream ops that are done already.
    activateUpstreamOperators();
    // always activate the downstream ops that we pushed data to
    activateDownstreamOperators();
    Assertion.invariant(_upstreamOpsToActivate.isEmpty());
    Assertion.invariant(_downstreamOpsToActivate.isEmpty());
    return t;
  }

  protected Transition waitingForComputation(){
    return isNewComputation() ? super.waitingForComputation() : Transition.INVALID_RUN;
  }

  protected void executingUserOp() {
    super.executingUserOp();
    // TODO review operator activation
    activateDownstreamOperators();
  }

  protected Transition executingMetaData(){
    _operatorRuntime.resetMonitoringState();
    // this operator may run concurrently with its upstream or downstream neighbour. if we only reactivate when the neighbour finishes
    // then we will lose the activations that may have happened until then and deadlock.
    _operatorRuntime.activateNotifications();

    boolean isReady = _operatorRuntime.isReadyForCall();
    _operatorRuntime.handleMetaData();
    // need to filter here because in the next line we make our decision based on whether
    // there are unfinished upstream ops.
    filterFinishedUpstreamOperators();

    // we need to track the reason why we did not go into executing and make sure that we
    // preserve it until EXECUTING_EPILOGUE in order to define the correct transition!
    boolean isBlocked = _operatorRuntime.isOperatorBlocked();
    // FIXME this is really tricky because the operator is backing out here although there
    // is data available on its incoming arc. in the case where the upstream operator has no
    // more additional data available, we are screwed because we do not get scheduled again.
    // i believe, this last option is only here because we wanted to give priority to
    // processing meta data. is this still required with the concept of special meta data
    // packets?! otherwise in such a case I have to
    // make sure that this section schedules itself again when it backs out.
    // FIXME What if there are many checkpoint markers arriving? Then this will always
    // reenter this state.
    if (!isTearDown() &&
            !_operatorRuntime.isOperatorBlocking() && !isBlocked && isReady)// && _upstreamOpsToActivate.isPresent())
    {
      _executedOperator = true;
      return Transition.EXECUTING_META_DATA_to_EXECUTING;
    } else {
      if (isBlocked) {
        // if I'm blocked then I always want to activate the operators downstream that
        // block me. no matter what!
        registerAllDownstreamBlocking();
        _backoffReason = BackoffReason.FULL_ARC;
      }
      // else if(!_upstreamOpsToActivate.isPresent())
      // {
      // // if we give priority to meta packets then we *have* to make sure we get scheduled
      // // again, because there might be data inside the arc left and *this* run might have
      // // been the notification for that. if we miss that dequeue then we might never get
      // // another notification again (if the data source is a blocking server socket for
      // // example).
      // _backoffReason = BackoffReason.META_PRIOTITY;
      // System.out.println("META_PRIORITY path");
      // }
      _executedOperator = false;
      return Transition.EXECUTING_META_DATA_to_EXECUTING_EPILOGUE;
    }
  }

  protected void reset() {
    _isBlocked = false;
    _noMoreDownstreamActivations = true;
    _nextActivation = null;
  }

  private void checkQuietOperator() {
    if (_upstreamOpsToActivate.isEmpty()
            && _downstreamOpsToActivate.isEmpty()
            && !_operatorRuntime.getOp().isSystemComponent()
            && _executedOperator
            && _operatorRuntime.wasLastPacket()) {
      Assertion.impossible("Nothing happened in operator: " + _operatorRuntime.getOp().getOperatorName() + "!\n" +
              "It means that your operator implementation just entered a live lock.\n" +
              "It neither retrieved from one of its input ports nor did it emit data to any of its output ports.\n" +
              "The framework now thinks the operator is done with its computation.\n" +
              "Try implementing IDone to signal explicitly to the framework when the operator is done.\n" +
              "If you already did then:\n" +
              "- here is some scheduler information:\n" +
              "\n ---> isBlocking? (Ports are in blocked state -  does not happen during processing - should be false) " + _operatorRuntime.isOperatorBlocking() +
              "\n ---> isOperatorBlocked (outgoing arcs are full) " + _operatorRuntime.isOperatorBlocked() +
              "\n ---> isQuantaExhausted? " + _operatorRuntime.isQuantaExhausted() +
              "\n- here is the operator's state for you for debugging:\n" +
              // TODO serialize properly!
              _operatorRuntime.getOp().getState());
    }
  }

  private boolean isNewComputation() {
    if (_operatorRuntime.getOp().getOperatorAlgorithm() instanceof SystemOperator && _operatorRuntime.getOp().getNumInputPorts() == 0)
      return true;
    else
      return 0 < _operatorRuntime.getOp().getInputPorts().stream().
              filter(inputPort -> inputPort.getIncomingArc().peek().get() instanceof ActivationMarker).
              count();
  }

  private void clearUpstreamNotifications() {
    for (InputPort p : _upstreamOpsToActivate)
      ((AbstractNotificationBasedArcImpl)p.getIncomingArc().getImpl()).enableUpstreamNotification();
    _upstreamOpsToActivate.clear();
  }

  private void registerAllDownstreamBlocking() {
    for (Arc downstream : _operatorRuntime.getOp().getGraphNodeOutputConnections()) {
      if (((AbstractNotificationBasedArcImpl) downstream.getImpl()).isBlocking()) {
        addDownstreamOpToBeActivated(downstream);
      }
    }
  }

//  @SuppressWarnings("unused")
//  private void assertInitializationPhase() {
//    assert _operatorRuntime.getProcessState() != SystemPhaseType.GRAPH_ANALYSIS
//            && _operatorRuntime.getProcessState() != SystemPhaseType.COMPUTATION : "Init on operator ("
//            + _operatorRuntime.getOperatorName()
//            + ") called in phase "
//            + _operatorRuntime.getProcessState().toString();
//  }

  /**
   * A computation is defined to be complete if and only if all the input ports have seen the
   * end of stream marker. In order to avoid requiring the operator to tell us when it is done,
   * we say in addition that if we ran that operator and no processing was done (no data was
   * pushed into the outgoing arcs) the operator must be done. We exclude the case where the
   * operator is being blocked by its downstream neighbors. <br>
   * Since we give priority to markers it might happen that all the below conditions are true
   * and it might look like the operator is done. But that is not true if we skipped the
   * operator algorithm! (The case where we handle markers but do not activate anybody
   * downstream is the arrival of the EOS!)
   *
   * @return
   */
  private boolean isComputationComplete() {
    // TODO implement default method into interface
    if (_operatorRuntime.getOp().getOperatorAlgorithm() instanceof IDone) {
      // no more guessing around
      return _operatorRuntime.wasLastPacket() && ((IDone) _operatorRuntime.getOp().getOperatorAlgorithm()).isComputationComplete();
    } else {
      boolean executed = _operatorRuntime.getProcessState() == SystemPhaseType.COMPUTATION ? _executedOperator : true;
//      System.out.println(_operatorRuntime + " : " + _operatorRuntime.wasLastPacket() + " : " + operatorIsWaitingForData() + " : " + executed);

      // FIXME the second call is soooo twisted! it is not checking the input but the output side!
      return _operatorRuntime.wasLastPacket() && operatorIsWaitingForData() && executed;
    }
  }

  /**
   * An operator is done with its computation if and only if it did not activate any downstream
   * operators although it is not blocked.
   *
   * @return
   */
  private boolean operatorIsWaitingForData() {
    if (_operatorRuntime.getOp().isSystemComponent()) {
      if (_operatorRuntime.getOp().getInputPorts().isEmpty()) {
        // for source system ops we need to check if they sent something at all downstream (not
        // matter the packet type).
        return _downstreamOpsToActivate.isEmpty() && !_operatorRuntime.isOperatorBlocked();
      } else {
        // normal system ops are done only depending on when all their input ports are done.
        // NOTE: it means that flows of system ops can not shut down only parts of their flow up
        // to a certain operator!
        return true;
      }
    } else {
      _isBlocked = _operatorRuntime.isOperatorBlocked();
      _noMoreDownstreamActivations = _downstreamOpsToActivate.isEmpty();
      _nextActivation = _noMoreDownstreamActivations ? null : _downstreamOpsToActivate.iterator().next().getOperatorName();
      return _noMoreDownstreamActivations && !_isBlocked;
    }
  }

  private boolean isTearDown() {
//    if (_operatorRuntime.getOp().getInputPorts().isEmpty()) {
      return _operatorRuntime.isTearDown();
//      return _operatorRuntime.getOp().getOperatorAdapter().isOperatorTearDown();
//    }
//
//    boolean done = true;
//    for (InputPort inPort : _operatorRuntime.getOp().getInputPorts()) {
//      if (inPort.getState() != PortState.CLOSED) {
//        done = false;
//      }
//    }
//    return done;
  }

  private void closeOutputPorts() {
    for (OutputPort out : _operatorRuntime.getOp().getOutputPorts()) {
      out.close();
    }
  }

  private void filterFinishedUpstreamOperators() {
    Iterator<InputPort> it = _upstreamOpsToActivate.iterator();
    while (it.hasNext()) {
      if (it.next().hasSeenLastPacket()) {
        it.remove();
      }
    }
  }

  private void activateUpstreamOperators() {
    if (!_operatorRuntime.getOp().getOperatorAdapter().isInputFavored()) {
      _upstreamOpsToActivate.clear();
      return;
    }

    filterFinishedUpstreamOperators();
    for (InputPort inPort : _upstreamOpsToActivate) {
      OperatorCore upstreamOp = inPort.getIncomingArc().getSource();
      ((AbstractNotificationBasedArcImpl) inPort.getIncomingArc().getImpl()).enableUpstreamNotification();

      // these operators are always only scheduled by downstream activation! otherwise we end up
      // with live locks.
      if (inPort.getIncomingArc().getType() != ArcType.FEEDBACK_EDGE) _operatorRuntime.activateOther(upstreamOp, false);
    }
    _upstreamOpsToActivate.clear();
  }

  private void activateDownstreamOperators() {
    if (!_operatorRuntime.getOp().getOperatorAdapter().isOutputFavored()) {
      _downstreamOpsToActivate.clear();
      return;
    }

    for (OperatorCore downstreamOp : _downstreamOpsToActivate) {
      // FIXME this is just not cool because it completely breaks the sovereinity of the
      // operators. These things should be given to the scheduler and it should actually perform
      // these activations. If the ops are not on this section then the scheduler should notify
      // the section scheduler.
      _operatorRuntime.activateOther(downstreamOp, true);
      // downstreamOp.activateOperator(downstreamOp, true);

      // FIXME this really wants the exact op!
      for (Arc a : downstreamOp.getGraphNodeInputConnections())
        ((AbstractNotificationBasedArcImpl) a.getImpl()).enableDownstreamNotification();

    }
    _downstreamOpsToActivate.clear();
  }

  /**
   * We say that the initialization of the input ports of this operator is complete if and only
   * if all ACTIVE input ports have been initialized.
   *
   * @return
   */
  private boolean completeInputPortInitialization() {
    boolean portInitCompleted = true;

    // prepare all input ports
    for (InputPort inPort : _operatorRuntime.getOp().getInputPorts()) {
      // we initialize non active ports ourselves
      if (!inPort.isActive()) {
        inPort.initialize();
        inPort.initComplete();
        continue;
      }

      if (inPort.getState() != PortState.INIT) {
        // port has already been initialized
        continue;
      }

      // this thing assumes that this will always be an end-of-init marker!
//      Object packet = _operatorRuntime.pollDataUninterruptably(inPort);
      Object packet = _operatorRuntime.pollData(inPort);
      if (packet != null) {
        throw new InvariantBroken();
      }

      // since the port state did not change no marker was dequeued and the init is not
      // complete. nevertheless we check all ports in order to guarantee forward progress.
      if (inPort.getState() == PortState.INIT) {
        // FIXME Why do we call the target operator at this point? We should just answer 'false'
        // here!
//        portInitCompleted = _operatorRuntime.isTargetOperatorBlocking() && portInitCompleted;
//        portInitCompleted = false && portInitCompleted;
        portInitCompleted = false;
      }
    }

    return portInitCompleted;
  }

//  protected void prepareInputPorts() {
//    // prepare all input ports
//    for (InputPort inPort : _operatorRuntime.getOp().getInputPorts()) {
//      if (inPort.getState() == PortState.INIT) {
//        inPort.initialize();
//      }
//    }
//
//    if (_operatorRuntime.getOp().getOutputPorts().isEmpty()) {
//      // the MetaTargetOperator needs this hook
//      _operatorRuntime.getOp().getOperatorAdapter().prepareInputPorts();
//    }
//
//  }
//
  protected void addUpstreamOpToBeActivated(InputPort targetPort) {
    // this already has the right implementation: it registers its own input port where the
    // incoming arc sits whose source should be activated!
    _upstreamOpsToActivate.add(targetPort);
    ((AbstractNotificationBasedArcImpl) targetPort.getIncomingArc().getImpl()).disableUpstreamActivation();
//    System.out.println("<-- op: " + _operatorRuntime.getOperatorName() + " >> " + targetPort.getIncomingArc().getSource());
  }

  protected void addDownstreamOpToBeActivated(Arc arc) {
    // FIXME this should not collect operators but arcs! these should be passed to the operator
    // scheduler to decide whether the downstream should activated. this would only be the case
    // when the arc is no inter-section arc. otherwise the operator scheduler has to pass this
    // arc upwards to the section scheduler which then has to perform the activation of the
    // section and the operator.
    _downstreamOpsToActivate.add(arc.getTarget());
    ((AbstractNotificationBasedArcImpl) arc.getImpl()).disableDownstreamActivation();
//    System.out.println("--> op: " + _operatorRuntime.getOperatorName() + " >> " + arc.getTarget());
  }

  protected boolean isOperatorExecuted() {
    return _executedOperator;
  }

  public void notifyBackoffReason(BackoffReason reason) {
    _backoffReason = reason;
//    System.out.println("op: " + _operatorRuntime.getOperatorName() + " backOffReason: " + reason);
  }

  protected BackoffReason getBackOffReason() {
    return _backoffReason;
  }

  public enum BackoffReason {
    OPERATOR_DECISION,
    NULL_DEQUEUE,
    FULL_ARC,
    META_PRIOTITY
  }

//  public enum OperatorState implements Serializable {
//    // Initialization states
//    INIT,
//    INIT_EPILOGUE,
//
//    // Computation states
//    EXECUTING_META_DATA,
//    EXECUTING,
//    EXECUTING_USER_OP,
//    EXECUTING_EPILOGUE,
//    WAITING_FOR_DATA,
//    CAN_PRODUCE_MORE_DATA,
//    QUANTA_EXHAUSTED,
//    FINISHING_COMPUTATION,
//
//    // Intermediate state
//    WAITING_FOR_COMPUTATION,
//
//    // Shutdown states
//    CLEAN_UP,
//    DONE
//  }
//
//  private enum Transition {
//    INIT_to_INIT_EPILOGUE(OperatorState.INIT, OperatorState.INIT_EPILOGUE),
//    INIT_EPILOGUE_to_INIT_EPILOGUE(OperatorState.INIT_EPILOGUE,
//            OperatorState.INIT_EPILOGUE),
//    INIT_EPILOGUE_to_WAITING_FOR_DATA(OperatorState.INIT_EPILOGUE,
//            OperatorState.WAITING_FOR_DATA),
//    INIT_EPILOGUE_to_WAITING_FOR_COMPUTATION(OperatorState.INIT_EPILOGUE,
//            OperatorState.WAITING_FOR_COMPUTATION),
//
//    WAITING_FOR_DATA_to_EXECUTING_META_DATA(OperatorState.WAITING_FOR_DATA,
//            OperatorState.EXECUTING_META_DATA),
//    CAN_PRODUCE_MORE_DATA_to_EXECUTING_META_DATA(OperatorState.CAN_PRODUCE_MORE_DATA,
//            OperatorState.EXECUTING_META_DATA),
//
//    EXECUTING_META_DATA_to_EXECUTING(OperatorState.EXECUTING_META_DATA,
//            OperatorState.EXECUTING),
//    EXECUTING_META_DATA_to_EXECUTING_EPILOGUE(OperatorState.EXECUTING_META_DATA,
//            OperatorState.EXECUTING_EPILOGUE),
//    QUANTA_EXHAUSTED_to_EXECUTING_META_DATA(OperatorState.QUANTA_EXHAUSTED,
//            OperatorState.EXECUTING_META_DATA),
//    FINISHING_COMPUTATION_to_EXECUTING(OperatorState.FINISHING_COMPUTATION,
//            OperatorState.EXECUTING),
//
//    EXECUTING_to_EXECUTING_EPILOGUE(OperatorState.EXECUTING,
//            OperatorState.EXECUTING_EPILOGUE),
//    EXECUTING_EPILOGUE_to_WAITING_FOR_DATA(OperatorState.EXECUTING_EPILOGUE,
//            OperatorState.WAITING_FOR_DATA),
//    EXECUTING_EPILOGUE_to_CAN_PRODUCE_MORE_DATA(OperatorState.EXECUTING_EPILOGUE,
//            OperatorState.CAN_PRODUCE_MORE_DATA),
//    EXECUTING_EPILOGUE_to_QUANTA_EXHAUSTED(OperatorState.EXECUTING_EPILOGUE,
//            OperatorState.QUANTA_EXHAUSTED),
//    EXECUTING_EPILOGUE_to_FINISHING_COMPUTATION(OperatorState.EXECUTING_EPILOGUE,
//            OperatorState.FINISHING_COMPUTATION),
//
//    WAIT_FOR_NEW_COMPUTATION(OperatorState.EXECUTING_EPILOGUE,
//            OperatorState.WAITING_FOR_COMPUTATION),
//    ENTER_COMPUTATION_STATE(OperatorState.WAITING_FOR_COMPUTATION, OperatorState.WAITING_FOR_DATA), // none-marker-based
//    EXECUTE_NEW_COMPUTATION(OperatorState.WAITING_FOR_COMPUTATION, OperatorState.EXECUTING_META_DATA), // marker-based
//    DANGLING_NOTIFICATION(OperatorState.WAITING_FOR_COMPUTATION, OperatorState.WAITING_FOR_COMPUTATION),
//
//    FINISH_ALL_PROCESSING(OperatorState.EXECUTING_EPILOGUE, OperatorState.CLEAN_UP),
//
//    FINALIZE(OperatorState.CLEAN_UP, OperatorState.DONE),
//    DONE(OperatorState.DONE, OperatorState.DONE),
//
//    LEAVE_OHUA_ENGINE(OperatorState.EXECUTING, OperatorState.EXECUTING_USER_OP),
//    RETURN_TO_OHUA_ENGINE(OperatorState.EXECUTING_USER_OP, OperatorState.EXECUTING);
//
//    private OperatorState _from, _to = null;
//
//    Transition(OperatorState fromState, OperatorState toState) {
//      _from = fromState;
//      _to = toState;
//    }
//
//    private void apply(NotificationBasedOperatorRuntime operator) {
//      assert operator.getOperatorState() == _from;
//      operator.setOperatorState(_to);
//      if (_from == _to) {
//        operator.incDanglingCallCount();
////        System.out.println("OP still waiting for computation: " + operator.getOperatorName());
//      } else operator.resetDanglingCallCount();
////      System.out.println(operator.getOperatorName() + ": "+  _from + " -> " + _to);
//    }
//  }

}
