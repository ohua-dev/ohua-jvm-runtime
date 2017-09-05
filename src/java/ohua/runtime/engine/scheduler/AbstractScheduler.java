/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.AbstractRuntime;
import ohua.runtime.engine.EngineMetaComponentCreator;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.InvariantBroken;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.operators.system.ProcessControlOperator;
import ohua.runtime.engine.operators.system.UserGraphExitOperator;
import ohua.runtime.engine.sections.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by sertel on 1/20/17.
 */
public abstract class AbstractScheduler<S extends AbstractOperatorRuntime, T extends ISectionRuntime> implements IRuntime, Runnable {
  protected ScheduledThreadPoolExecutor _executor = null;
  protected AbstractRuntime.RuntimeState<S> _runtimeState;
  protected Throwable _exception = null;

  protected Map<Section, T> _runtimes = new HashMap<>();
  private Consumer<Optional<Throwable>> _onDone = e -> { };
  private Thread _me = null;
  private Set<OperatorCore> _operatorsToLaunch = null;
  /**
   * A counter for sections that are currently running (updated in SectionTask.beforeExection() and
   * SectionTask.afterExecution()).
   */
  private AtomicInteger _runningSectionsCount = new AtomicInteger(0);
  private AtomicLong _executionCount = new AtomicLong(0);
  private ReentrantLock _finishLock = new ReentrantLock();
  private Condition _awaitAllSectionsFinished = _finishLock.newCondition();

  public void initialize(AbstractRuntime.RuntimeState<S> runtimeState, RuntimeProcessConfiguration config) {
    _runtimeState = runtimeState;
    _executor = createExecutor(config.getCoreThreadPoolSize());
    assignSchedulingPriorities();
    setUpInterSectionArcs(config);
    _runtimeState._sectionGraph.getEntireSectionWorld().stream().forEach(s -> _runtimes.put(s, createSectionRuntime(s, config)));
    setUpExternalInput();
  }

  private void setUpExternalInput(){
    OperatorCore controlOp = _runtimeState._sectionGraph.findOperator(EngineMetaComponentCreator.MetaOperator.PROCESS_CONTROL.opName());
    ((ProcessControlOperator) controlOp.getOperatorAlgorithm()).setExternalInput(new ProcessControlOperator.ExternalMetaInput() {
      private ConcurrentLinkedQueue<IMetaDataPacket> _packets = new ConcurrentLinkedQueue<>();

      @Override
      public boolean isInputAvailable() {
        return !_packets.isEmpty();
      }

      @Override
      public IMetaDataPacket poll() {
        return _packets.poll();
      }

      @Override
      public void push(LinkedList<IMetaDataPacket> packets) {
        _packets.addAll(packets);
      }
    });
  }

  private void setUpInterSectionArcs(RuntimeProcessConfiguration config) {
    // assure thread-safety
    setUpInterSectionArcs(_runtimeState._sectionGraph.getInterSectionArcs(), config);
    setUpInterSectionArcs(_runtimeState._sectionGraph.getSystemInterSectionArcs(), config);
  }

  abstract protected void setUpInterSectionArcs(Collection<Arc> arcs, RuntimeProcessConfiguration config);

  private void assignSchedulingPriorities() {
    // assign priorities to the sections
    new SectionPriorityAdvisor().assignPriorities(_runtimeState._sectionGraph);
    assignPriorities(_runtimeState._sectionGraph, _runtimeState._opRuntimes);
  }

  private void assignPriorities(SectionGraph sectionGraph, Map<OperatorCore, S> opRuntimes){
    Set<OperatorCore> start = sectionGraph.getEntireOperatorWorld().stream().filter(OperatorCore::isSystemOutputOperator).collect(Collectors.toSet());
    for(int prio = AbstractOperatorRuntime.DEFAULT_SCHEDULING_PRIORITY + 1; !start.isEmpty(); ++prio){
      final int p = prio;
      start.forEach(op -> opRuntimes.get(op).setGraphPriority(p));
      start = start.stream()
              .flatMap(op -> op.getAllSucceedingGraphNodes(false).stream())
              .distinct()
              .filter(op -> op.getAllPreceedingGraphNodes(false)
                      .stream()
                      .noneMatch(pop -> opRuntimes.get(pop).getGraphPriority() == AbstractOperatorRuntime.DEFAULT_SCHEDULING_PRIORITY))
              .collect(Collectors.toSet());
    }
  }

  abstract protected T createSectionRuntime(AbstractSection s, RuntimeProcessConfiguration config);

  protected ScheduledThreadPoolExecutor createExecutor(int coreThreadPoolSize) {
    return new ScheduledThreadPoolExecutor(coreThreadPoolSize);
  }

  @Override
  public void launch(Set<OperatorCore> ops) {
    _operatorsToLaunch = ops;
  }

  @Override
  public void start(SystemPhaseType systemPhase) {
    _me = new Thread(this, "Ohua Scheduler");
    _me.setDaemon(true);
    _me.start();
  }

  public final void run() {
    launchSystemPhase();

    try {
      runSystemPhase();
    } catch (Throwable t) {
      _exception = t;
    } finally {
      _onDone.accept(_exception != null ? Optional.of(_exception) : Optional.empty());
    }
  }

  protected void launchSystemPhase() {
    _operatorsToLaunch.stream().forEach(this::activate);
    _runningSectionsCount.set(0);
    _executionCount.set(_executor.getCompletedTaskCount());
    _runtimeState._sectionGraph.getEntireSectionWorld().stream().map(_runtimes::get).forEach(ISectionRuntime::startNewSystemPhase);
  }

  protected abstract void cancelPeriodicTasks();

  protected final void runSystemPhase() {
    scheduleSections();

    cancelPeriodicTasks();
    waitForExitingSections();
    sweepFlowGraph();
  }

  protected abstract void scheduleSections();

  /**
   * With periodic tasks it can happen that there is state left over in the arcs of the flow
   * graph that got introduced when computation had already finished and the process was already
   * shutting down. This routine makes sure that we do not carry any state over to the next
   * system phase. <br>
   * We not have to have a distributed algorithm here in order to do so because we do not
   * perform operator specific tasks. (Note that all flow graph segments on distributed notes
   * will have this section scheduler and will therefore probably also have something like a
   * MetaTarget op to understand when they are done.)
   */
  private void sweepFlowGraph() {
    _runtimeState._sectionGraph.getAllArcs().stream().forEach(Arc::sweep);
  }

  // TBD should we always wait for tasks to finish before we shutdown/return control?
  private void waitForExitingSections() {
    _finishLock.lock();
    try {
      while (_executionCount.get() < _executor.getCompletedTaskCount() // make sure at least one task was executed.
              && _runningSectionsCount.get() > 0) {
        _awaitAllSectionsFinished.awaitUninterruptibly();
      }
    } finally {
      _finishLock.unlock();
    }
  }

  protected final void startedSection(){
    _runningSectionsCount.getAndIncrement();
  }

  protected final void finishedSection(boolean done){
    int runningSections = _runningSectionsCount.decrementAndGet();
    if (runningSections < 1) {
      if (done) {
        _finishLock.lock();
        try {
          _awaitAllSectionsFinished.signalAll();
        } finally {
          _finishLock.unlock();
        }
      }
    }
  }

  protected void terminate(Throwable t){
    _runningSectionsCount.set(-10);
    _exception = t;
    _executor.shutdownNow();
  }

  @Override
  public void teardown() {
    _executor.shutdown();
  }

  @Override
  public void onDone(Consumer<Optional<Throwable>> onDone) {
    _onDone = onDone;
  }

  protected final boolean isSystemPhaseCompleted() {
    // if we ever extend the system graph beyond the exit op (for whatever reason) then this needs to change most likely.
    return ((UserGraphExitOperator) _runtimeState._sectionGraph.getUserGraphExitSection().getOperator().getOperatorAlgorithm()).systemPhaseCompleted();
  }

}
