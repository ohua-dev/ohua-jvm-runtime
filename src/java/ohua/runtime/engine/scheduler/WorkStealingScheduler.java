/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

/**
 * A work stealing scheduler based on the fork/join framework from Java.
 * @author sertel
 * 
 */
// TODO This scheduler is not so easy. It needs a slot for sections because this is unit of work
// that wants to be scheduled. However, in order to do so, we need incoorporate a shared data
// structure for work slots (which are essentially the arcs) such that we can identify the guy
// that fill the last slot. Because this guy needs to finally kick off the pending work on this
// section.
  @Deprecated
public class WorkStealingScheduler extends TaskScheduler
{
//  private ForkJoinPool _pool = null;
//
//  public static class SectionFJTask extends ForkJoinTask<Work>
//  {
//
//    private WorkTask _workTask = null;
//
//    /**
//     * The initial work on a source section.
//     * @param work
//     */
//    public SectionFJTask(Section section, List<Work> work) {
//      _workTask = new WorkTask(section, work);
//    }
//
//    @Override public Work getRawResult() {
//      throw new UnsupportedOperationException();
//    }
//
//    @Override protected void setRawResult(Work value) {
//      throw new UnsupportedOperationException();
//    }
//
//    @Override protected boolean exec() {
//      _workTask.beforeExecution();
//      try {
//        _workTask.call();
//      }
//      catch(Exception e) {
//        e.printStackTrace();
//        super.completeExceptionally(e);
//        return false;
//      }
//      List<Work> inCompleteWork = _workTask.inCompleteWork();
//      // TODO What happens when the section needs to back off? Should we handle this here or
//      // should we give the incomplete work as the result of this task to the next one that
//      // depends on this task?
//      return true;
//    }
//
//    /**
//     * One gets the reporting here for this work to be ready. What we need to do is to spawn a
//     * new task that would execute this work but only if the previous work on this task was
//     * done!
//     * @param id
//     * @param work
//     */
//    // TODO the notifier must report to this task instead of the scheduler.
//    public void notifyWorkReady(OperatorID id, Work work) {
//      // TODO
//    }
//  }
//
//  public static class DependentSectionTask extends SectionFJTask
//  {
//    private SectionFJTask _dependent = null;
//
//    public DependentSectionTask(Section section, List<Work> work, SectionFJTask dependency) {
//      super(section, work);
//      _dependent = dependency;
//    }
//
//    @Override protected boolean exec() {
//      _dependent.join();
//      // TODO I have not understood how back-pressure works in this model!
//      return super.exec();
//    }
//  }
//
//  @Override public void initialize(SectionGraph graph, int coreThreadPoolSize, boolean concurrentSchedulingEnabled) {
//    super.initialize(graph, coreThreadPoolSize, concurrentSchedulingEnabled);
//
//    // TODO
//    _pool = new ForkJoinPool(coreThreadPoolSize);
//  }
//
//  public void start(SystemPhaseType systemPhase) {
//    // TODO
//  }
//
//  // protected void notifyWorkReady(OperatorID id, Work work) {
//  // WorkQueue q = _pendingWork.get(id);
//  // q.add(work);
//  // if(q.getInputDataCount() > work.limit()) {
//  // work.full();
//  // }
//  // }
//
//  /**
//   * Incomplete work must be enqueued into the work queues preserving the order. There can be
//   * incomplete work from multiple operators. Therefore, we revert the list and always prepend
//   * to the work queues.
//   *
//   * @param inCompleteWork
//   */
//  // protected void notifyIncompleteWork(List<Work> inCompletedWork) {
//  // Collections.reverse(inCompletedWork);
//  // for(Work work : inCompletedWork) {
//  // _pendingWork.get(work.getReference()).prependUnfinished(work);
//  // }
//  // }
  
}
