/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import ohua.runtime.engine.AbstractRuntime;
import ohua.runtime.engine.flowgraph.elements.ConcurrentArcQueue;
import ohua.runtime.engine.flowgraph.elements.operator.AsynchronousArcImpl;
import ohua.runtime.engine.flowgraph.elements.operator.NotificationBasedOperatorRuntime;
import ohua.runtime.engine.scheduler.AbstractScheduler;
import ohua.runtime.engine.scheduler.OperatorScheduler;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.DeadlockException;
import ohua.runtime.engine.exceptions.InvariantBroken;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.AbstractSection.SectionID;
import ohua.runtime.engine.operators.system.UserGraphExitOperator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles the full scheduling of the sections. <br>
 * It is better to have an own thread handling this in order to avoid direct concurrency among
 * the sections!
 */
public class SectionScheduler extends AbstractScheduler<NotificationBasedOperatorRuntime, NotificationBasedSectionRuntime> implements ActivationService {
  // statistics
  public long _scheduledOpsCount = 0;
  private Logger _logger = Logger.getLogger(getClass().getCanonicalName());
  /**
   * Queue of tasks to be submitted to be executed submitted to the executor below.
   * This thread will wait on this queue for new tasks to arrive.
   */
  private PriorityBlockingQueue<OhuaTask> _readyTasks = null;

  /**
   * Control flags for the execution of this section scheduler.
   */
  private volatile boolean _done = false;
  /**
   * A map of the futures for the periodic tasks. We use it to cancel them once we shut down execution.
   */
  private Map<SectionID, ScheduledFuture<?>> _scheduledTaskList = new HashMap<>();

  /**
   * We keep track of the last n tasks that finished in order to analyze deadlocks.
   */
  private ConcurrentLinkedDeque<FinishedSectionExecution> _lastFinishedTasks = new ConcurrentLinkedDeque<>();
  /**
   * The number of tasks submitted to the thread pool executor that has not yet finished.
   */
  private AtomicInteger _numScheduledTasks = new AtomicInteger(0);

  private boolean _concurrentSchedulingEnabled = false;

  private Map<Section, NotificationBasedSectionRuntime> _runtimes = new HashMap<>();

  @Override
  public void initialize(AbstractRuntime.RuntimeState<NotificationBasedOperatorRuntime> runtimeState, RuntimeProcessConfiguration config) {
    super.initialize(runtimeState, config);
    init(runtimeState._sectionGraph.getAllSections().size(), config.getCoreThreadPoolSize(), config.isConcurrentSchedulingEnabled());
    runtimeState._sectionGraph.getAllSections().stream().forEach(s -> _runtimes.get(s).setSchedulingQuanta(config.getSchedulingQuanta()));
    _runtimeState._sectionGraph.getEntireOperatorWorld().stream().forEach(o -> {
      NotificationBasedOperatorRuntime runtime = _runtimeState._opRuntimes.get(o);
      runtime.setRuntimes(_runtimeState._opRuntimes); // needed for activations
      runtime.defineQuanta(config.getOperatorQuanta());
    });
    _runtimeState._sectionGraph.getAllArcs().stream().forEach(a -> {
      AsynchronousArcImpl impl = (AsynchronousArcImpl) a.getImpl();
      impl.setDefaultDownstreamActivation((k,l) -> _runtimeState._opRuntimes.get(k.getSource()).addDownstreamOpToBeActivated(l));
      impl.setDefaultUpstreamActivation((k,l) -> _runtimeState._opRuntimes.get(k.getTarget()).addUpstreamOpToBeActivated(l.getTargetPort()));
    });
  }

  private void init(int maxQueueSize,
//                    ProcessObserver caller,
                    int coreThreadPoolSize,
                    boolean concurrentSchedulingEnabled) {
    // this is assumes that there can not be more entries than sections exist.
    _readyTasks = new PriorityBlockingQueue<>(maxQueueSize, new TaskPriorityComparator());
    _executor = createExecutor(coreThreadPoolSize, new SectionRejectedHandler());
    _executor.prestartAllCoreThreads();

    prepareFlowGraph(concurrentSchedulingEnabled);
    _runtimeState._sectionGraph.getEntireSectionWorld()
            .stream()
            .forEach(s -> _runtimes.put(s, new NotificationBasedSectionRuntime(s, _runtimeState._opRuntimes)));
  }

  protected ScheduledThreadPoolExecutor createExecutor(int coreThreadPoolSize,
                                                       SectionRejectedHandler rejectedHandler) {
    return new ScheduledThreadPoolExecutor(coreThreadPoolSize, rejectedHandler);
  }

  protected void launchSystemPhase() {
    resetVariables();
    super.launchSystemPhase();
  }

  protected void cancelPeriodicTasks() {
    for (ScheduledFuture<?> periodicTask : _scheduledTaskList.values()) {
      periodicTask.cancel(false);
    }
  }

  private void resetVariables() {
    _done = false;
    _exception = null;
    _readyTasks.clear();
    _scheduledTaskList.clear();
    _numScheduledTasks.set(0);
  }

  public void teardown() {
    // the very last section might still be in but with DONE status!
    assert _readyTasks.size() < 2;
    if (RuntimeProcessConfiguration.LOGGING_ENABLED) _logger.log(Level.ALL, "Done!!!");
    _executor.shutdown();
  }

  private void prepareFlowGraph(boolean concurrentSchedulingEnabled) {
    setUpEnhancedScheduling(concurrentSchedulingEnabled);
  }

  protected void setUpEnhancedScheduling(boolean enableConcurrentActivation) {
    if (enableConcurrentActivation)
      enableConcurrentActivation();
    _concurrentSchedulingEnabled = enableConcurrentActivation;
  }

  private void enableConcurrentActivation() {
    for (Section section : _runtimeState._sectionGraph.getAllSections()) {
      if (!section.isSystemComponent()) {
        for (Arc arc : section.getOutgoingArcs()) {
          if (!arc.getSourcePort().isMetaPort()
                  && !arc.getTargetPort().isMetaPort()) {
            ((AsynchronousArcImpl) arc.getImpl()).registerArcEventListener(new ConcurrentPipelineScheduling(arc, this, _runtimeState._opRuntimes.get(arc.getTarget())));
          }
        }
      }
    }
  }

  /**
   * Retrieves sections one after the other from the queue. Will be waiting on the queue of the
   * executor if there is no more space available in the task queue.
   */
  protected void scheduleSections() {
    OhuaTask currentSectionTask = getNextSectionFromQueue();
    while (currentSectionTask != null && !_done && _exception == null) {
      if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
        _logger.info("Task dequeued: " + currentSectionTask);
      }
      currentSectionTask.execute();
      currentSectionTask = getNextSectionFromQueue();
    }
  }

  /**
   * This function retrieves a section from the queue. For the case where there are no more
   * sections in the queue it will wait until the next section is being enqueue. The very last
   * section to finish its processing will interrupt this thread (and the waiting on this
   * queue). (see SectionExecutor.afterExecute() for more details!)
   *
   * @return
   */
  protected OhuaTask getNextSectionFromQueue() {
    OhuaTask currentSection;
    try {
      while (true) {
        currentSection = _readyTasks.poll(3, TimeUnit.SECONDS);
        if (currentSection == null && deadlockCheck()) continue;
        else break;
      }
    } catch (InterruptedException e) {
      // we got interrupted after the last section has finished its processing. therefore we
      // need to get out of the loop above and return to the caller.
      currentSection = null;
      System.out.println("Caught interrupt!");
    } catch (IllegalMonitorStateException ex) {
      return null;
    }

    return currentSection;
  }

  public void activateSection(Activation toActivate) {
    activateSections(toActivate);
  }

  private List<ActivationRequirement> _activationRequirements = new LinkedList<>();

  public void addActivationRequirement(ActivationRequirement requirement){
    _activationRequirements.add(requirement);
  }

  // new interface
  protected int activateSections(Activation toActivate) {
    toActivate._downStreamActivations.removeAll(toActivate._upStreamActivations);
    activateSections(toActivate._downStreamActivations, ActivationType.DOWNSTREAM);
    activateSections(toActivate._upStreamActivations, ActivationType.UPSTREAM);
    return toActivate._downStreamActivations.size() + toActivate._upStreamActivations.size();
  }

  private void activateSections(Set<OperatorCore> toActivate, ActivationType type) {
    Set<NotificationBasedSectionRuntime> sections = new HashSet<>();
    for (OperatorCore op : toActivate) {
      if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
        _logger.info("Looking up section for op: " + op);
      }
      if(_activationRequirements.stream().allMatch(a -> a.apply(op, type)))
        sections.add(_runtimes.get(_runtimeState._sectionGraph.findParentSection(op.getId())));
    }
    activateSections(sections);
  }

  /**
   * A function that activates all the passed sections for scheduling. They will be pushed into
   * the queue of the SectionScheduler and scheduled according to their priority and the
   * availability of resources.
   *
   * @param toActivate
   */
  private void activateSections(Collection<NotificationBasedSectionRuntime> toActivate) {
    if (toActivate.isEmpty()) {
      return;
    }
    // this is tricky because we actually want to support output-favored scheduling of those
    // sections, but when we just stupidly schedule them here then the first sections activated
    // will win! we have to perform a presorting here!
    PriorityQueue<NotificationBasedSectionRuntime> q =
            new PriorityQueue<>(toActivate.size(), new SectionPriorityComparator());
    q.addAll(toActivate);
    // Collections.sort(toActivate, new SectionPriorityComparator());
    for (NotificationBasedSectionRuntime sectionToActivate : q) {
      activateSection(sectionToActivate);
    }
  }

  /**
   * Since we use use the HashMap.putIfAbsent() function to avoid duplicate sections in the
   * scheduling queue, we also need to update this HashMap once a section is done with
   * processing. This function is called by the SectionExecutor for every task that has finished
   * execution.
   *
   * @param finished
   */
  protected boolean afterExecution(SectionScheduler.FinishedSectionExecution finished) {

    // FIXME this wants to be a flag on the section! (= the section state)!
    boolean freeSection = true;

    // System.out.println("Section done.");
    super.finishedSection(_done);
//    int runningSections = _runningSectionsCount.decrementAndGet();
//    if (runningSections < 1) {
//      if (_done) {
//        _finishLock.lock();
//        try {
//          _awaitAllSectionsFinished.signalAll();
//        } finally {
//          _finishLock.unlock();
//        }
//      }
//    }

    if (_concurrentSchedulingEnabled) {
      // be sensitive to notifications again
      for (Arc arc : finished._section._section.getIncomingArcs()) {
        ((AsynchronousArcImpl) arc.getImpl()).getSourceListeners().stream().filter(l -> l instanceof ConcurrentPipelineScheduling).forEach(l -> ((ConcurrentPipelineScheduling) l).resetNotifications());
      }

      for (Arc arc : finished._section._section.getOutgoingArcs()) {
        ((AsynchronousArcImpl) arc.getImpl()).getTargetListeners().stream().filter(l -> l instanceof ConcurrentPipelineScheduling).forEach(l -> ((ConcurrentPipelineScheduling) l).resetNotifications());
      }
    }

    _lastFinishedTasks.add(finished);
    // sadly this size call is a bit expensive, but I don't have a better solution for now.
    if (_lastFinishedTasks.size() > 10) _lastFinishedTasks.poll();

    _numScheduledTasks.decrementAndGet();

    return freeSection;
  }

  private boolean deadlockCheck() {
    // note that a task is submitted to "readyTasks" only before the "numScheduledTasks" is counted down.
    if (_numScheduledTasks.get() < 1 &&
            _readyTasks.size() < 1
//            && !_sectionGraph.hasPeriodicUserSections()
            ) {
      /**
       * Deadlock: This happens when an operator got scheduled but did not use the data that is available to it.
       * When the operator returns without doing anything then no new notifications are being triggered and the execution dies.
       */
      _exception = new DeadlockException(_runtimeState._sectionGraph, new LinkedList<>(_lastFinishedTasks), _runtimeState._opRuntimes);
      return false;
    }
    else {
      return true;
    }
  }

  /**
   * Cleans the notifications for the given section, a step done only when a section reports
   * DONE status!
   *
   * @param finished
   */
  protected void beforeExecution(NotificationBasedSectionRuntime finished) {
    // if(!finished.getIncomingArcs().isPresent())
    // {
    // System.out.println("scheduled section load: "
    // + finished.getIncomingArcs().get(0).getMappedArc().getLoadEstimate());
    // }
//    _runningSectionsCount.getAndIncrement();
    startedSection();
  }

  /**
   * For now our goal is to avoid for running multiple instances of one section concurrently!
   * Therefore we have an invariant here stating that the scheduling queue is a set-based (no
   * duplicates allow). Since the tasks (threads) themselves will update this queue, we need
   * atomic behavior in checking whether an section is already present in the queue and if not
   * to set it. The PriorityBlockingQueue implementation does not allow for this behavior. So we
   * use a ConcurrentHashMap that keeps track of the items that are currently in the scheduling
   * queue. In addition to that think of the case where section X is taken from the scheduling
   * queue and passed to the scheduler. It is not allowed to push section X into the queue,
   * before the current executor task for section X has finished processing! (Otherwise we might
   * again end up in the situation where we run multiple threads for the same section.)
   * Therefore the Map will also guard the scheduling queue as the map is updated once the
   * current task on section X has finished its computation.
   */
  protected boolean activateSection(NotificationBasedSectionRuntime toActivate, long schedulingDelay) {
    if (_done) {
      _logger.finest("Section " + toActivate
              + " did NOT make it into the task queue because the scheduler is done.");
      return false;
    }

    // only a single thread can win the call below
    if (toActivate.canActivate()) {
      return schedule(toActivate, schedulingDelay);
    } else {
      return false;
    }
  }

  protected boolean schedule(NotificationBasedSectionRuntime toActivate, long schedulingDelay) {
    boolean execute = true;

    if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
      _logger.info("Section " + toActivate + " made it into task queue. delay: " + schedulingDelay);
    }

    if (execute) {
      // add it to the queue
      _readyTasks.put(new ScheduledSectionTask(this, toActivate, schedulingDelay));
      return true;
    } else {
      return false;
    }
  }

  protected boolean activateSection(NotificationBasedSectionRuntime toActivate) {
    return activateSection(toActivate, 0);
  }

  @Override
  protected void setUpInterSectionArcs(Collection<Arc> arcs, RuntimeProcessConfiguration config) {
    Assertion.invariant(config.getInterSectionArcBoundary() > 0);
    int arcActivationMark = config.getArcActivationMark();
    for(Arc arc : arcs) {
      ((AsynchronousArcImpl) arc.getImpl()).exchangeQueue(new ConcurrentArcQueue(config.getInterSectionQueueImpl()));
      arc.setArcBoundary(config.getInterSectionArcBoundary());
      ((AsynchronousArcImpl) arc.getImpl()).setEnqueueWatermark(config.getArcEnqueueWatermark());
      ((AsynchronousArcImpl) arc.getImpl()).setActivationMark(arcActivationMark);
    }
  }

  @Override
  protected NotificationBasedSectionRuntime createSectionRuntime(AbstractSection s, RuntimeProcessConfiguration config) {
    return new NotificationBasedSectionRuntime((Section) s, _runtimeState._opRuntimes);
  }

  protected void done(Section lastFinishedSection) {
    if (!(lastFinishedSection.getOperators().get(0).getOperatorAlgorithm() instanceof UserGraphExitOperator)) {
      throw new InvariantBroken();
    }

    _done = true;
    _readyTasks.put(new OhuaTask() {
      @Override
      public int getSchedulingPriority() {
        return 1000;
      }

      @Override
      public void execute() {
        // never executed because of the _done flag. task just used to get _me out of the waiting state on the queue.
        Assertion.impossible();
      }
    });
  }

  protected void terminate(Throwable t) {
    super.terminate(t);
    _readyTasks.put(new OhuaTask() {
      @Override
      public int getSchedulingPriority() {
        return 1000;
      }

      @Override
      public void execute() {
        // never executed because of the _crashed flag. task just used to get _me out of the waiting state on the queue.
        Assertion.impossible();
      }
    });
  }

  public Throwable getException() {
    return _exception;
  }

  @Override
  public void activate(OperatorCore op) {
    NotificationBasedOperatorRuntime opRuntime = _runtimeState._opRuntimes.get(op);
    opRuntime.activateOperator(opRuntime);
    activateSection(_runtimes.get(_runtimeState._sectionGraph.findParentSection(op.getId())));
  }

  interface OhuaTask {
    int getSchedulingPriority();

    void execute();
  }

  public static class ScheduledSectionTask implements OhuaTask {
    protected NotificationBasedSectionRuntime _sectionRuntime = null;
    private SectionScheduler _scheduler = null;
    private long _schedulingDelay = 0;

    public ScheduledSectionTask(SectionScheduler scheduler, NotificationBasedSectionRuntime toActivate, long schedulingDelay) {
      _scheduler = scheduler;
      _sectionRuntime = toActivate;
      _schedulingDelay = schedulingDelay;
    }

    public void execute() {
//      if (_sectionRuntime._section.isPeriodic()) {
//        // this call is NON-BLOCKING! It will just kick off the section(thread) and then go
//        // ahead!
//        scheduleDelayedTask(_sectionRuntime,
//                _sectionRuntime.getSchedulingDelay(),
//                _sectionRuntime.getSchedulingDelayTimeUnit());
//      } else
      if (_schedulingDelay != 0) {
        // this call is NON-BLOCKING! It will just kick off the section(thread) and then go
        // ahead!
        scheduleDelayedTask(_sectionRuntime, _schedulingDelay, TimeUnit.MILLISECONDS);
      } else {
        // System.out.println("running sections: " + _runningSectionsCount);
        // this call is NON-BLOCKING! It will just kick off the section(thread) and then go
        // ahead!
        submit(_scheduler._executor);
        _scheduler._numScheduledTasks.incrementAndGet();
      }
      _scheduler._scheduledOpsCount++;
    }

    protected void submit(ThreadPoolExecutor executor) {
      executor.submit(new SectionTask(_sectionRuntime, _scheduler));
    }

    private void scheduleDelayedTask(NotificationBasedSectionRuntime sectionRuntime, long delay, TimeUnit units) {
      if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
        _scheduler._logger.info("Scheduled periodic task: " + sectionRuntime.toString());
        _scheduler._logger.info("task delay: " + delay + units);
      }
      ScheduledFuture<?> future = _scheduler._executor.schedule(new SectionTask(sectionRuntime, _scheduler), delay, units);
      _scheduler._scheduledTaskList.put(sectionRuntime._section.getSectionID(), future);
    }

    public int getSchedulingPriority() {
      return _sectionRuntime.getSchedulingPriority();
    }
  }

  public class SectionRejectedHandler implements RejectedExecutionHandler {

    public void rejectedExecution(Runnable arg0, ThreadPoolExecutor arg1) {
      assert !arg1.isShutdown();

      // we want the calling thread (= the SectionScheduler) to wait on the queue until a space
      // becomes available!
      try {
        arg1.getQueue().put(arg0);
      } catch (InterruptedException e) {
        // do nothing here although there is no reason we would interrupt this process yet!
        throw new InvariantBroken();
      }
    }

  }

  private class AbstractSectionComparator {
    protected int compare(int schedulingPrioArg0, int schedulingPrioArg1) {
      int result;

      // apply rule 1
      result = rule1(schedulingPrioArg0, schedulingPrioArg1);
      if (result != 0) {
        return result;
      }

      // apply rule 2
      result = rule2(schedulingPrioArg0, schedulingPrioArg1);

      return result;
    }

    /**
     * Rule 1: Sections with a higher priority will be placed closer to the head of the queue.
     *
     * @return
     */
    private int rule1(int schedulingPrioArg0, int schedulingPrioArg1) {
      if (schedulingPrioArg0 > schedulingPrioArg1) {
        return -1;
      }
      if (schedulingPrioArg0 < schedulingPrioArg1) {
        return 1;
      }

      return 0;
    }

    /**
     * Rule 2: Sections that have longer waiting time will be placed closer to the head of the
     * queue.
     *
     * @return
     */
    private int rule2(int schedulingPrioArg0, int schedulingPrioArg1) {
      return 1;
    }

  }

  public class SectionPriorityComparator extends AbstractSectionComparator implements Comparator<NotificationBasedSectionRuntime> {
    public int compare(NotificationBasedSectionRuntime arg0, NotificationBasedSectionRuntime arg1) {
      return super.compare(arg0.getSchedulingPriority(), arg1.getSchedulingPriority());
    }
  }

  // FIXME there is some work here: create the notion of a task and define this rules in an
  // abstract way so they can be shared with the operator scheduler. furthermore use a real
  // enqueue timestamp in order to compare the wait-times!
  public class TaskPriorityComparator extends AbstractSectionComparator
          implements Comparator<OhuaTask> {
    public int compare(OhuaTask arg0, OhuaTask arg1) {
      return super.compare(arg0.getSchedulingPriority(), arg1.getSchedulingPriority());
    }
  }

  public static class FinishedSectionExecution {
    public NotificationBasedSectionRuntime _section;
    public int _activations;
    public long _start;
    public long _finished;
    public boolean _executed;
    public OperatorScheduler.SchedulerResult _result;

    FinishedSectionExecution(NotificationBasedSectionRuntime section, int activations, long start, long finished, boolean executed, OperatorScheduler.SchedulerResult result) {
      _section = section;
      _activations = activations;
      _start = start;
      _finished = finished;
      _executed = executed;
      _result = result;
    }
  }


  public interface ActivationRequirement extends BiFunction<OperatorCore, ActivationType, Boolean> {
    // nothing
  }

  public enum ActivationType {
    DOWNSTREAM,
    UPSTREAM
  }

  public static class Activation {
    public boolean _isPendingNotification = false;
    public Section _run;
    public Set<OperatorCore> _downStreamActivations;
    public Set<OperatorCore> _upStreamActivations;

    public Activation(Section run) { _run = run; }

  }
}
