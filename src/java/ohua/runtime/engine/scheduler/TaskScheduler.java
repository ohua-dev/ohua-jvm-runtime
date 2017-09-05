/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.sections.SectionScheduler;

/**
 * A purely task-based scheduler.
 * <p>
 * A note on concurrent scheduling: Concurrent execution of two adjacent operators on different
 * sections is achieved by having a higher arc boundary than task size!
 * @author sertel
 * 
 */
// FIXME this thing should probably not derive from the section scheduler!
@Deprecated
public class TaskScheduler extends SectionScheduler
{
//  private static class ScheduledWorkTask extends ScheduledSectionTask
//  {
//    private List<Work> _work = null;
//
//    public ScheduledWorkTask(SectionScheduler sectionScheduler,
//                             Section toActivate,
//                             long schedulingDelay,
//                             List<Work> work)
//    {
//      super(sectionScheduler, toActivate, schedulingDelay);
//      _work = work;
//    }
//
//    @Override protected void submit(ThreadPoolExecutor executor) {
//      executor.submit(new WorkTask(super._sectionRuntime, _work));
//    }
//  }
//
//  private Map<OperatorID, WorkQueue> _pendingWork = new ConcurrentHashMap<OperatorID, WorkQueue>();
//  private ExecutionTracker _execTracker = new ExecutionTracker();
//
//  // TODO make this a runtime configuration parameter
//  private SchedulingAlgorithm _schedulingAlgorithm = new BasicSchedulingAlgorithm();
//  private PendingWorkOverview _pendingWorkOverview = null;
//
//  private ReadReference<Arc> _externalArcRef = null;
//
//  @Override public void initialize(SectionGraph graph, int coreThreadPoolSize, boolean concurrentSchedulingEnabled) {
//    super.initialize(graph, coreThreadPoolSize, concurrentSchedulingEnabled);
//    _pendingWorkOverview = new PendingWorkOverview(new ReadReference<Map<OperatorID, WorkQueue>>() {
//      public Map<OperatorID, WorkQueue> get() {
//        return _pendingWork;
//      }
//    }, new ReadReference<ExecutionTracker>() {
//      public ExecutionTracker get() {
//        return _execTracker;
//      }
//    }, new ReadReference<SectionGraph>() {
//      public SectionGraph get() {
//        return _sectionGraph;
//      }
//    });
//  }
//
//  protected ScheduledThreadPoolExecutor createExecutor(int coreThreadPoolSize,
//                                                       SectionRejectedHandler rejectedHandler)
//  {
//    // TODO the rejected handler is actually that performs the waiting on the queue for
//    // available threads.
//    final TaskScheduler scheduler = this;
//    return new TaskExecutor(coreThreadPoolSize, new ReadReference<ExecutionTracker>() {
//      public ExecutionTracker get() {
//        return _execTracker;
//      }
//    }, new ReadReference<TaskScheduler>() {
//      public TaskScheduler get() {
//        return scheduler;
//      }
//    });
//  }
//
//  /**
//   * This method is executed on the caller thread (process manager) and starts the scheduler
//   * thread.
//   */
//  public void start(SystemPhaseType systemPhase) {
//    adjustWorkChunkSize(systemPhase);
//    super.start(systemPhase);
//  }
//
//  private void adjustWorkChunkSize(SystemPhaseType systemPhase) {
//    for(Section section : _sectionGraph.getEntireSectionWorld()) {
//      for(Arc arc : section.getOutgoingArcs()) {
//        int workChunkSize =
//            systemPhase == SystemPhaseType.COMPUTATION && !arc.getSource().isSystemComponent() ? 10 : 1;
//        ((TaskBasedAsynchronousArc) arc.getImpl()).setWorkChunkSize(workChunkSize);
//      }
//    }
//  }
//
//  /**
//   * This method is called to perform the actual scheduling.
//   */
//  protected void runSystemPhase() {
//    super.runSystemPhase();
//
//    // rebuild the list again when we are done such that new requests can be captured.
//    preparePendingWork();
//  }
//
//  private void preparePendingWork() {
//    for(OperatorCore op : _sectionGraph.getEntireOperatorWorld()) {
//      Set<ArcID> arcs = new HashSet<ArcID>();
//      for(Arc arc : op.getGraphNodeInputConnections()) {
//        arcs.add(arc.getArcId());
//      }
//      _pendingWork.put(op.getId(), new WorkQueue(arcs));
//      op.disableForeignActivations();
//    }
//    OperatorCore controlOp = _sectionGraph.findOperator(MetaOperator.PROCESS_CONTROL.opName());
//    _pendingWork.put(controlOp.getId(), new WorkQueue(Collections.singleton(_externalArcRef.get().getArcId())));
//  }
//
//  /**
//   * Called during scheduler initialization.
//   */
//  @Override
//  protected void setUpEnhancedScheduling(boolean enableConcurrentScheduling) {
//    for(Section section : _sectionGraph.getEntireSectionWorld()) {
//      for(Arc arc : section.getOutgoingArcs()) {
//        arc.setImpl(new TaskBasedAsynchronousArc(arc, 1));
//        WorkReadyNotifier notifier = new WorkReadyNotifier(this);
//        arc.registerArcEventListener(notifier);
//        arc.getSourcePort().registerEventListener(notifier);
//      }
//    }
//
//    final OperatorCore controlOp = _sectionGraph.findOperator(MetaOperator.PROCESS_CONTROL.opName());
//    final WorkReadyNotifier notifier = new WorkReadyNotifier(this);
//    final Arc arc = new ExternalInputArc(controlOp);
//    ExternalMetaInput metaInput = new ExternalMetaInput() {
//      {
//        arc.setImpl(new TaskBasedAsynchronousArc(arc, 1));
//        arc.registerArcEventListener(notifier);
//      }
//
//      @Override public boolean isInputAvailable() {
//        return !arc.isQueueEmpty();
//      }
//
//      @Override public IMetaDataPacket poll() {
//        Maybe<Object> maybe = arc.getData();
//        // old semantics
//        return maybe.isPresent() ? (IMetaDataPacket) maybe.get() : null;
//      }
//
//      @Override public void push(LinkedList<IMetaDataPacket> packets) {
//        arc.enqueueBatch(packets);
//      }
//    };
//    ((ProcessControlOperator) controlOp.getOperatorAlgorithm()).setExternalInput(metaInput);
//    _externalArcRef = new ReadReference<Arc>() {
//      public Arc get() {
//        return arc;
//      }
//    };
//    preparePendingWork();
//  }
//
//  protected ScheduledSectionTask getNextSectionFromQueue() {
//    while(true) {
//      List<Work> work = nextWork();
//      if(work.isEmpty()) {
//        /*
//         * When operators are done they get removed from the _pendingWork list. So per
//         * definition the computation is complete whenever this list is empty.
//         */
//        if(_pendingWork.isEmpty()) {
//          // computation is done
//          return null;
//        }
//        else {
//          // no new work currently available
//          _execTracker.awaitReadyOperators();
//          continue;
//        }
//      }
//      else {
//        Section section = _sectionGraph.findParentSection(work.get(0).getReference());
//        return new ScheduledWorkTask(this, section, 0, work);
//      }
//    }
//  }
//
//  // TODO needs to be run concurrently not only when a task is needed!
//  // TODO this takes the work only for one single operator. however, a section might have
//  // multiple inputs and therefore it would be wise to also gather this pending work before
//  // kicking off the section.
//  private List<Work> nextWork() {
//    OperatorID op = _schedulingAlgorithm.schedule(_pendingWorkOverview, _sectionGraph);
//    if(op == null) {
//      return Collections.emptyList();
//    }
//    else {
//      Assertion.invariant(_execTracker.isReady(op));
//      _execTracker.block(op);
//      // TODO define concrete task size here! this helps when reported sizes vary as in the case
//      // of meta operators.
//      List<Work> work = _pendingWork.get(op).get();
//      Assertion.invariant(work.size() > 0);
//      return work;
//    }
//  }
//
//  /*
//   * Notifications start here
//   */
//
//  protected void notifyWorkReady(OperatorID id, Work work) {
//    WorkQueue q = _pendingWork.get(id);
//    q.add(work);
//  }
//
//  /**
//   * Incomplete work must be enqueued into the work queues preserving the order. There can be
//   * incomplete work from multiple operators. Therefore, we revert the list and always prepend
//   * to the work queues.
//   *
//   * @param inCompletedWork
//   */
//  protected void notifyIncompleteWork(List<Work> inCompletedWork) {
//    Collections.reverse(inCompletedWork);
//    for(Work work : inCompletedWork) {
//      _pendingWork.get(work.getReference()).prependUnfinished(work);
//    }
//  }
//
//  /**
//   * Called when the operator has finished its computation and therefore needs to be evicted
//   * from the _pendingWork map.
//   * @param op
//   */
//  protected void notifyFinishedOperator(OperatorCore op) {
//    WorkQueue q = _pendingWork.remove(op.getId());
//    Assertion.invariant(q.getInputDataCount() == 0);
//  }
//
//  protected void beforeExecution(AbstractSection finished) {
//    // TODO
//  }
//
//  protected boolean afterExecution(AbstractSection finished) {
//    // TODO
//    return true;
//  }
  
}
