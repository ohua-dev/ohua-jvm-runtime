/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.scheduler.OperatorScheduler;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by sertel on 1/26/17.
 */
public class NotificationBasedOperatorRuntime extends AbstractOperatorRuntime {

  /*
 * The quanta of an operator is a fairness property that is independent of the resources
 * available (arcs). It should count the interactions (dequeues and enqueues) of the operator
 * with the framework done by an operator and force it to return to the scheduler
 * periodically. This makes sure that operators with a higher priority get their share on
 * execution because operators on a section might not be directly connected to each other!
 */
  public int OPERATOR_QUANTA = 1000;

  // FIXME scheduling information that does not belong here!
  protected volatile boolean _returnToScheduler = false;

  private OperatorScheduler _myScheduler = null;
  private OperatorStateMachine _stateMachine;

  /*
 * Note that the quanta is not related to the data packets an operator retrieved it is related
 * to the interactions with its input and output ports!
 */
  private int _quanta = OPERATOR_QUANTA;
  private boolean _activateForeignOperators = true;

  // default: I'm in the scheduler queue, so at least one port has data and I can run.
  private Function<NotificationBasedOperatorRuntime, Boolean> _callConvention = (op) -> true;

  /**
   * Number of times this operator get scheduled but actually was already done with computation.
   */
  private int _danglingCallCount = 0;

  private Map<OperatorCore, NotificationBasedOperatorRuntime> _runtimes;

  public NotificationBasedOperatorRuntime(OperatorCore op, RuntimeProcessConfiguration config){
    super(op, config);
    _stateMachine = new OperatorStateMachine(this);
    // TODO set all operator configuration here
    config.aquirePropertiesAccess(properties ->
            _callConvention =
                    (Function<NotificationBasedOperatorRuntime, Boolean>)
                            properties.getOrDefault("calling-convention", _callConvention));
  }

  public void setRuntimes(Map<OperatorCore, NotificationBasedOperatorRuntime> runtimes){
    _runtimes = runtimes;
  }

//  public void defineCallingConvention(Function<NotificationBasedOperatorRuntime, Boolean> callConvention){
//    _callConvention = callConvention;
//  }

  public boolean isReadyForCall(){
    return _callConvention.apply(this);
  }

  public Function<NotificationBasedOperatorRuntime, Boolean> getCallConvention(){
    return _callConvention;
  }

  public void defineQuanta(int quanta){
    OPERATOR_QUANTA = quanta;
  }

  public int usedQuanta() { return _quanta; }

  public void runOperatorStep() {
    _stateMachine.reset();
    _stateMachine.runTransition();
  }

  public void resetQuanta() {
    _quanta = OPERATOR_QUANTA;
  }

  protected void resetMonitoringState() {
    // reset here because otherwise we might end up in an endless loop if we do not go into
    // the EXECUTING state but our QUANTA is exhausted!
    notifyBackoffReason(OperatorStateMachine.BackoffReason.OPERATOR_DECISION);
  }

  public void addUpstreamOpToBeActivated(InputPort targetPort) {
    _stateMachine.addUpstreamOpToBeActivated(targetPort);
  }

  public void addDownstreamOpToBeActivated(Arc arc) {
    _stateMachine.addDownstreamOpToBeActivated(arc);
  }

  public void setOperatorScheduler(OperatorScheduler scheduler) {
    _myScheduler = scheduler;
  }

  public void activateOperator(NotificationBasedOperatorRuntime owner) {
    _myScheduler.setActiveOperator(owner);
  }

  public void activateOperator(OperatorCore owner, boolean downstreamActivation) {
    if(!owner.isSystemComponent()) {
      if(downstreamActivation) {// activation comes from an upstream operator
//        int max = 1;
//        for(OperatorCore upstream : owner.getAllPreceedingGraphNodes(false)) {
//          if(!upstream.isSystemComponent()) {
//            max = Math.max(upstream.getSchedulingPriority(), max);
//          }
//        }
//        owner.setSchdedulingPriority(max + 1);
      } else {// activation comes from a downstream operator
//        int min = 1;
//        for(OperatorCore downstream : owner.getAllSucceedingGraphNodes(false)) {
//          if(!downstream.isSystemComponent()) {
//            min = Math.min(downstream.getSchedulingPriority(), min);
//          }
//        }
//        owner.setSchdedulingPriority(min - 1);
      }
    }
    activateOperator(_runtimes.get(owner));
  }

  /**
   * This function is supposed to be used in the operator logic to find out whether there will
   * be more packets arriving on this port or not.
   *
   * The pattern for the operator code should look something like this:
   *
   * while( pollData(0) != null) { ... }
   *
   * if(wasLastPacket())
   *
   * return OperatorReturnState.DONE_PROCESSING;
   *
   * else
   *
   * return OperatorReturnState.MORE_DATA_NEEDED;
   *
   *
   * @return
   */
  protected final boolean wasLastPacket() {
    if(_op.getInputPorts().isEmpty()) {
      return isSourceOperatorWasLastPacket();
    }

    for(InputPort inPort : _op.getInputPorts()) {
      if(inPort.isMetaPort() && !inPort.isUpstreamControlled()) {
        // don't take input ports into account that deliver complimentary data only
      } else {
        if(!inPort.hasSeenLastPacket()) {
          return false;
        }
      }
    }

    return true;
  }

  protected boolean isSourceOperatorWasLastPacket() {
    return _op.getOperatorAdapter().isSourceOperatorWasLastPacket();
  }

  /**
   * An operator is considered to be blocked if and only if all its output ports are blocked AND
   * it respects the boundary of the arcs!
   * @return
   */
  // FIXME this decision is influenced by the processing of the downstream operator and might change
  // immediately after this check. we should definitely not do this!
  public boolean isOperatorBlocked() {
    // output ops are never blocked!
    if(_op.getOutputPorts().isEmpty()) {
      return false;
    }

    for (OutputPort outPort : _op.getOutputPorts()) {
//      if(!outPort.isMetaPort()){
      for (Arc outArc : outPort.getOutgoingArcs()) {
        if (!((AbstractNotificationBasedArcImpl) outArc.getImpl()).isBlocking()) {
          return false;
        }
      }
//      }
    }

    return true;
  }

  /**
   * An operator is considered to be blocked if and only if all its output ports are blocked!
   * Note that the arc state is not connected to the port state. (Please see
   * isOperatorBlocking()) A blocked output port is a means for "outer entities" such as for
   * instance marker handlers to take a certain amount of control of the operator.
   *
   * @return
   */
  protected boolean isOperatorBlocking() {
    // output ops are never blocked!
    if (_op.getOutputPorts().isEmpty()) {
//      return isTargetOperatorBlocking();
      return false;
    }

    for (int i = 0; i < _op.getOutputPorts().size(); i++) {
      OutputPort outPort = _op.getOutputPorts().get(i);
      if (outPort.getState() != AbstractPort.PortState.BLOCKED) {
        return false;
      }
    }

    return true;
  }

  protected int skimMetaData(InputPort inPort) {
    // Note: This interaction accounts for at least one quanta point even if no data was
    // retrieved!
    int skimmedCount = super.skimMetaData(inPort);
    _quanta -= (skimmedCount + 1);
    return skimmedCount;
  }

  /**
   * Only skim for meta data packets that are independent of the state of this operator. (Later
   * on we will introduce a better operator state management and then we can get rid of this
   * function again.)
   */
  protected int skimSpecialMetaData(InputPort inPort) {
    // Note: This interaction accounts for at least one quanta point even if no data was
    // retrieved!
    int skimmedCount = super.skimSpecialMetaData(inPort);
    _quanta -= (skimmedCount + 1);
    return skimmedCount;
  }

  /**
   * This function is supposed to be used in the operator logic to find out whether there will
   * be more packets arriving on this port or not.
   * <p>
   * Can return null when no data packet is available. By checking for null the operator can
   * determine whether there is data to be processed or not.
   * <p>
   * The pattern for the operator code should look something like this:
   *
   * while( pollData(0) != null) { ... }
   *
   * if(wasLastPacket())
   *
   * return OperatorReturnState.DONE_PROCESSING;
   *
   * else
   *
   * return OperatorReturnState.MORE_DATA_NEEDED;
   *
   *
   * @return
   */
  private Maybe<Object> _returnControl = Maybe.empty();
  public final Maybe<Object> pollData(InputPort inputPort) {
    if(isQuantaExhausted() || _returnToScheduler) {
      return _returnControl;
    }
    _quanta--;

    Maybe<Object> dequeuedDataPacket = super.pollData(inputPort);

    if(!dequeuedDataPacket.isPresent()) notifyBackoffReason(OperatorStateMachine.BackoffReason.NULL_DEQUEUE);
    return dequeuedDataPacket;
  }

  /**
   * Pushes the data packet into all outgoing arcs of the output port identified by the given
   * index.
   * @param data
   * @return
   */
  public boolean pushData(OutputPort outPort, Object data) {
    boolean canEnqueueMore = super.pushData(outPort, data);
    if(!canEnqueueMore) notifyBackoffReason(OperatorStateMachine.BackoffReason.FULL_ARC);
    return handlePortReturnStatus(canEnqueueMore).handlePushDataReturnValue();
  }

  public void enqueueMetaData(OutputPort outPort, IMetaDataPacket packet){
    super.enqueueMetaData(outPort, packet);
    assert outPort.getOwner() == _op;
    outPort.getOutgoingArcs().stream().forEach(this::addDownstreamOpToBeActivated);
  }

  protected boolean isReturnToScheduler() {
    return _returnToScheduler;
  }

  /**
   * We need to refine this notion: An operator is said to have finished its computation iff all
   * of the following conditions hold:
   * <ul>
   * <li>all input ports received the EOS AND (-> catches ops with no output ports)
   * <li>all output ports are inactive AND (-> catches ops with no input ports)
   * <li>they recently were switched from active to inactive, that is the operator was executed.
   * <ul>
   * @return
   */
  public boolean hasFinishedComputation() {
    for(InputPort inPort : _op.getInputPorts()) {
      if(!inPort.hasSeenLastPacket()) return false;
    }

    for(OutputPort outPort : _op.getOutputPorts()) {
      if(outPort.isActive()) return false;
    }
    return _quanta < OPERATOR_QUANTA;
  }

  protected void finishOutputPorts() {
    super.finishOutputPorts();
    _quanta =- _op.getNumOutputPorts();
  }

  /**
   *
   * @return
   */
  public void notifyBackoffReason(OperatorStateMachine.BackoffReason reason) {
    _stateMachine.notifyBackoffReason(reason);
  }

  public OperatorStateMachine.BackoffReason getBackOffReason(){
    return _stateMachine.getBackOffReason();
  }

  /**
   * Calling this method will enforce a scheduling cycle. However, the operator is put back into
   * the scheduling queue.
   */
  public void yield() {
    _quanta = -1;
  }

  public void activateOther(OperatorCore op, boolean downstream) {
    boolean foreign = isForeign(_runtimes.get(op));
    if(foreign && !_activateForeignOperators) {
      return;
    }

    _runtimes.get(op).activateOperator(op, downstream);

    if(foreign) {
      _myScheduler.notifyForeignOperatorActivation(op, downstream);
    }
  }

  private boolean isForeign(NotificationBasedOperatorRuntime op) {
    return _myScheduler != op._myScheduler;
  }

  public void disableForeignActivations() {
    _activateForeignOperators = false;
  }

  protected void activateNotifications(){
    for (Arc a : _op.getGraphNodeOutputConnections())
      ((AbstractNotificationBasedArcImpl)a.getImpl()).enableDownstreamNotification();
    for (Arc a : _op.getGraphNodeInputConnections())
      ((AbstractNotificationBasedArcImpl)a.getImpl()).enableUpstreamNotification();
  }

  protected void activateDownstreamNotifications() {
    for (Arc a : _op.getGraphNodeOutputConnections())
      ((AbstractNotificationBasedArcImpl)a.getImpl()).enableDownstreamNotification();
  }

  protected void incDanglingCallCount() { _danglingCallCount++; }
  protected void resetDanglingCallCount() { _danglingCallCount = 0; }
  public int getDanglingCallCount() { return _danglingCallCount; }

  public String deadlockAnalysis(){
    StringBuffer analysis = new StringBuffer();
    analysis.append("+++++++++++++++++++++++++++\n");
    analysis.append("Operator: " + this + "\n");
    analysis.append("Operator.backOffReason: " + getBackOffReason() + "\n");
    analysis.append("Operator.operatorState: " + getOperatorState() + "\n");
    analysis.append("Operator.danglingCallCount: " + getDanglingCallCount() + "\n");
    analysis.append("Operator.wasOperatorBlocked: " + _stateMachine._isBlocked + "\n");
    analysis.append("Operator.noMoreDownstreamActications: " + _stateMachine._noMoreDownstreamActivations + "\n");
    analysis.append("Operator.nextActivation: " + _stateMachine._nextActivation + "\n");
    analysis.append("Operator.isOperatorBlocking: " + isOperatorBlocking() + "\n");
    analysis.append("Operator.executed: " + _stateMachine.isOperatorExecuted() + "\n");
    analysis.append("Operator.quanta: " + _quanta + "\n");
    _op.getInputPorts().stream().map(InputPort::deadlockAnalysis).forEach(s -> analysis.append(s));
    _op.getOutputPorts().stream().map(OutputPort::deadlockAnalysis).forEach(s -> analysis.append(s));
    return analysis.toString();
  }

  /**
   * Handles the return status of the output port that we have pushed data to. The status will
   * be handed back into the operator author code to derive the return value of the operator run
   * itself.
   * </p>
   * Only used by the DAAPI.
   * @param returnStatus
   * @return
   */
  protected PushDataReturnValue handlePortReturnStatus(boolean returnStatus) {
    _quanta--;
    if(isQuantaExhausted() || _returnToScheduler)
    // if((isQuantaExhausted() && isSystemOutputOperator()) || _returnToScheduler)
    {
      return PushDataReturnValue.RETURN_CONTROL;
    }

    if(!returnStatus) {
      return PushDataReturnValue.BOUNDARY_REACHED;
    }

    return PushDataReturnValue.BOUNDARY_NOT_YET_REACHED;
  }

  protected final boolean isQuantaExhausted() {
    return _quanta < 1;
  }

//  public static final class OperatorsComparator implements Comparator<OperatorCore> {
//    public int compare(OperatorCore o1, OperatorCore o2) {
//      return o1.getId().compareTo(o2.getId());
//    }
//  }

}
