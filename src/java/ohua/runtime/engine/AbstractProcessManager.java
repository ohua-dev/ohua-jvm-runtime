/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.operators.system.ProcessControlOperator;
import ohua.runtime.engine.points.PacketFactory;
import ohua.runtime.engine.flowgraph.elements.ArcID.ArcIDGenerator;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID.OperatorIDGenerator;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;
import ohua.runtime.engine.flowgraph.elements.operator.PortID.PortIDGenerator;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.operators.system.UserGraphExitOperator;
import ohua.runtime.engine.sections.AbstractSection.SectionIDGenerator;
import ohua.util.Tuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractProcessManager implements
                                            IInternalProcessManager,
                                            ISystemEventListener
{

  static{
    OperatorFactory.getInstance().registerSystemOperator("ProcessController", ProcessControlOperator.class, ProcessControlOperator.description());
  }

  protected Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  private volatile SystemPhaseType _systemPhase = SystemPhaseType.INITIALIZATION;
  private DataFlowProcess _process = null;

  // FIXME is this still needed?
  private ReentrantLock _pendingCallbacksLock = new ReentrantLock();
  private Condition _pendingCallbacksCondition = _pendingCallbacksLock.newCondition();
  private AtomicInteger _pendingCallbacks = new AtomicInteger(0);

  private EngineEvents _event = null;

  // FIXME belongs also into the process.
  private RuntimeProcessConfiguration _runtimeConfiguration = new RuntimeProcessConfiguration();

  /**
   * Marks a running process.<br>
   * Once we run multiple processes in parallel this will be a list (or another component
   * responsible for this)! Mind that with the new threading concept it is even possible to
   * schedule sections that do not belong to the same process by one and the same section
   * scheduler!
   */
  private Optional<Throwable> _runtimeFailure = Optional.empty();

  private ReentrantLock _pendingEngineCallbackLock = new ReentrantLock();
  private Condition _pendingEngineCallbackCondition = _pendingEngineCallbackLock.newCondition();

  /**
   * We let requesters wait iff we do process data, as this is asynchronous call that this flag
   * guards.
   */
  private AtomicBoolean _schedulerDone = new AtomicBoolean(true);

  private AbstractRuntime _runtime = null;

  public AbstractProcessManager(DataFlowProcess process, RuntimeProcessConfiguration runtimeConfiguration) {
    _runtimeConfiguration = runtimeConfiguration;
    init(process);
  }

  public static void resetCounters() {
    OperatorIDGenerator.resetCounter();
    PortIDGenerator.resetCounter();
    ArcIDGenerator.resetCounter();
    SectionIDGenerator.restCounter();
  }

  private void init(DataFlowProcess process) {
    _process = process;
  }

  public void initializeProcess() {
    _runtime = _runtimeConfiguration.getRuntime();
    _systemPhase = SystemPhaseType.INITIALIZATION;
    Tuple<OperatorCore, OperatorCore> controlAndExit = _runtime.initializeRuntime(_process.getGraph());
    _process._processControl = controlAndExit.first();
    _process._exit = controlAndExit.second();
    _runtime.onDone(e -> {
      // I know that the cast below is redundant but out of some reason my javac did not want to understand
      // that the type of e is Optional<Throwable>. It claimed it is Object only.
      _runtimeFailure = (Optional<Throwable>)e;
      notifyOnDoneStatus();
    });
    finalizeSystemPhase();
  }

  // FIXME used by the SubGraphLoader only
//  public void initializeFlowGraph() {
//    Set<OperatorCore> ops = new HashSet<>(_process.getGraph().getContainedGraphNodes());
//
//    // static initialization routines
//    initializeArcs();
//    prepareAndEnterComputationState(ops);
//
//    // perform the initialization of the operators
//    FlowGraphInitialization.prepareAndEnterComputationState(ops);
//    FlowGraphInitialization.activateOperators(ops);
//  }

  protected final void determineFlowNature() {
    List<PortID> needEOS = new ArrayList<>();
    for(OperatorCore op : _process.getGraph().getContainedGraphNodes()) {
      if(op.isSystemOutputOperator()) {
        PortID entrancePort =
            op.getInputPort("meta-input").getIncomingArc().getSourcePort().getPortId();
        needEOS.add(entrancePort);
      }
    }
    _process.setEOSNeeded(needEOS);

    _process.setProcessNature(ProcessNature.SOURCE_DRIVEN);

    if(RuntimeProcessConfiguration.LOGGING_ENABLED) {
      _logger.info("Flow nature: " + _process.getProcessNature());
    }
  }


  public void runFlow() {
    runProcessStartUp();
    runDataPhase();
  }

  // FIXME should be private!
  public void runProcessStartUp() {
    _systemPhase = SystemPhaseType.COMPUTATION;
  }

  public void runDataPhase() {
    LinkedList<IMetaDataPacket> packets = new LinkedList<>();
    packets.add(PacketFactory.createActivationMarkerPacket(_systemPhase));

    if(RuntimeProcessConfiguration.LOGGING_ENABLED) _logger.info("Process nature: " + _process.getProcessNature());
    switch(_process.getProcessNature())
    {
      case SOURCE_DRIVEN:
        packets.add(PacketFactory.createEndSignalPacket(2, _systemPhase));
        break;
      case USER_DRIVEN:
        // create EOS signals for ops with source driven input.
        if(!_process.getEOSNeeded().isEmpty()) {
          packets.add(PacketFactory.createConditionalEndSignalPacket(2, _systemPhase, _process.getEOSNeeded()));
        }
        // the user needs to decide when to stop the flow
    }

    prepareNewSystemPhase(packets);
    // _scheduler.activateNonblockingIO(_runtimeConfiguration.getAdvancedNonblockingIO());
    processData();
  }

  public void tearDownProcess() {
    _systemPhase = SystemPhaseType.TEARDOWN;
    finalizeSystemPhase();
  }

  public void cleanUp() {
    resetCounters();
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) _logger.log(Level.ALL, "Clean up done in ProcessManager");
  }

  protected final void prepareNewSystemPhase(LinkedList<IMetaDataPacket> markers) {
    _process.inject(markers);

    ((UserGraphExitOperator) _process._exit.getOperatorAlgorithm()).startNewSystemPhase();

//    Set<OperatorCore> toBeActivated = activateAdditionalSystemComponents(_process.getSectionGraph(), _systemPhase);
    _runtime.launch(Collections.singleton(_process._processControl));
//    _runtime.launch(toBeActivated);
  }

  // TODO make sure that the ProcessController is never being accessed concurrently.
  public void submitFlowInput(LinkedList<IMetaDataPacket> input) {
    _process.inject(input);
    _runtime.activate(_process._processControl);
  }

  public void processData() {
    _process.setState(ProcessState.RUNNING);
    _schedulerDone.set(false);
    _runtime.start(_systemPhase);
  }

  public void awaitSystemPhaseCompletion() throws Throwable {
    waitForProcessesToFinish();

    if(_runtimeFailure.isPresent()) {
      throw _runtimeFailure.get();
    }
  }

  /**
   * Let's the "ProcessManager" thread wait for the processes to finish processing their data,
   * before returning to the calling program!
   */
  private void waitForProcessesToFinish() {
    _pendingEngineCallbackLock.lock();
    try {
      while(!_schedulerDone.get()) {
        _pendingEngineCallbackCondition.awaitUninterruptibly();
      }
    }
    finally {
      _pendingEngineCallbackLock.unlock();
    }
  }

  public void finishComputation() {
    if(_process.getProcessNature() == ProcessNature.USER_DRIVEN) {
      LinkedList<IMetaDataPacket> packets = new LinkedList<>();
      packets.add(PacketFactory.createEndSignalPacket(2, _systemPhase));
      _process.inject(packets);
      activate("ProcessController");
    }
  }

  /**
   * Called by the threading framework part when all processing has finished.
   */
  public void notifyOnDoneStatus() {
    finalizeSystemPhase();

    _pendingEngineCallbackLock.lock();
    try {
      _schedulerDone.set(true);
      _pendingEngineCallbackCondition.signalAll();
    }
    finally {
      _pendingEngineCallbackLock.unlock();
    }
  }

  private void finalizeSystemPhase() {
    _process.setState(ProcessState.IDLE);
    switch(_systemPhase)
    {
      case INITIALIZATION:
        determineFlowNature();
        break;
      case COMPUTATION:
        break;
      case TEARDOWN:
        _runtime.teardown();
        cleanUp();
        _process.setState(ProcessState.DONE);
        break;
    }
  }

  public DataFlowProcess getProcess() {
    return _process;
  }

  public void activate(OperatorID operatorID) {
    if(operatorID == null) {
      throw new IllegalArgumentException();
    }

    OperatorCore op = _process.getGraph().getOperator(operatorID);
    _runtime.activate(op);
  }

  public void activate(String operatorName) {
    if(operatorName == null) {
      throw new IllegalArgumentException();
    }

    OperatorCore op = _process.getGraph().getOperator(operatorName);
    _runtime.activate(op);
  }

  public void notifyOnEvent(EngineEvents event) {
    if(event != _event) {
      return;
    }

    int pendingCallbacks = _pendingCallbacks.decrementAndGet();

    if(pendingCallbacks > 0) {
      return;
    }

    _pendingCallbacksLock.lock();
    try {
      _pendingCallbacksCondition.signalAll();
    }
    finally {
      _pendingCallbacksLock.unlock();
    }
  }

  protected RuntimeProcessConfiguration getRuntimeProcessConfiguration() {
    return _runtimeConfiguration;
  }
}
