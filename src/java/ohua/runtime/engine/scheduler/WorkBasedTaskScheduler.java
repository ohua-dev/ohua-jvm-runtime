/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.AbstractRuntime;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.DeadlockException;
import ohua.runtime.engine.exceptions.WrappedRuntimeException;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.operators.system.ProcessControlOperator;
import ohua.runtime.engine.sections.AbstractSection;
import ohua.runtime.engine.sections.Section;
import ohua.util.Tuple;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Created by sertel on 1/20/17.
 * <p>
 * This scheduler works based on the following abstraction of a section:
 * A section is defined as an opaque box with incoming and outgoing arcs, very much as an operator.
 * When the incoming arcs have data then they are listed with pending input work.
 * Once the upstream ops of the incoming arcs are done, it goes to a state where it only has pending source work.
 * It may also happen that the ops that are target to the incoming arcs are already done. In this case,
 * the first ops encountered when traversing the graph downstream are registered as having pending source work.
 * They are not listed as having pending input work because the whole section is in a "pending source work" state
 * and this scheduler does not manage the data among section. It manages the data in between them.
 * The according operator scheduler is responsible for finding an efficient schedule for the operators on a section.
 */
public class WorkBasedTaskScheduler extends AbstractScheduler<WorkBasedOperatorRuntime, WorkBasedSectionRuntime> {

  public static final String SCHEDULING_ALGO = "task.scheduler.algo";
  public static final ISchedulingAlgorithm DEFAULT_SCHEDULING_ALGO = (gr, sw, possibleWork) -> possibleWork.keySet().stream().findFirst().get().getOp();

  private Map<OperatorCore, Set<Arc>> _penalized = new HashMap<>();
  private LinkedBlockingQueue<BooleanSupplier> _doneTasks = new LinkedBlockingQueue<>();
  private Map<OperatorCore, SchedulerState> _work = new HashMap<>();

  private ISchedulingAlgorithm _schedAlgo = DEFAULT_SCHEDULING_ALGO;

  public static Consumer<OperatorCore> TRACE = op -> {};

  private int availableResources;

  public static final String DESIRED_WORK_SIZE = "task.scheduler.work-size";
  private int _desiredWorkSize = 1;

  public void initialize(AbstractRuntime.RuntimeState<WorkBasedOperatorRuntime> runtimeState, RuntimeProcessConfiguration config) {
    super.initialize(runtimeState, config);
    config.aquirePropertiesAccess(props -> {
      Object algo = props.get(SCHEDULING_ALGO);
      if(algo != null){
        if(algo instanceof ISchedulingAlgorithm){
          _schedAlgo = (ISchedulingAlgorithm) algo;
        }else{
          throw new IllegalArgumentException("Submitted scheduling algo does not implement " + ISchedulingAlgorithm.class.getName());
        }
      }else{
        // nothing
      }

      Object ws = props.getOrDefault(DESIRED_WORK_SIZE, config.getInterSectionArcBoundary());
      if (ws instanceof Integer)
        _desiredWorkSize = (Integer) ws;
      else RuntimeProcessConfiguration.throwExcept(ws);
    });
    availableResources = config.getCoreThreadPoolSize();
    if (availableResources <= 0) throw new IllegalStateException("No resources available");
  }

  @Override
  protected ScheduledThreadPoolExecutor createExecutor(int coreThreadPoolSize) {
    return new Executor(coreThreadPoolSize);
  }

  @Override
  protected void setUpInterSectionArcs(Collection<Arc> arcs, RuntimeProcessConfiguration config) {
    // nothing because we use one arc type for both scheduler hierarchies
  }

  @Override
  protected WorkBasedSectionRuntime createSectionRuntime(AbstractSection s, RuntimeProcessConfiguration config) {
    return new WorkBasedSectionRuntime(s, _runtimeState._opRuntimes, config);
  }

  @Override
  protected void launchSystemPhase() {
    initializeSchedulerState(); // do before activations of ops to be launched!
    super.launchSystemPhase();
  }

  private void initializeSchedulerState() {
    _work.clear();
    _runtimeState._sectionGraph.getEntireOperatorWorld()
            .stream()
            .forEach(o -> _work.put(o, new SchedulerState()));
  }

  @Override
  protected void cancelPeriodicTasks() {
    // TODO should be done by the abstract class
  }

  /**
   * In this scheduler implementation this is only called during start-up or whenever data is injected into
   * the running system from outside.
   *
   * @param op
   */
  @Override
  public void activate(OperatorCore op) {
    // TBD should this interface then be converted to reflect this assertion?
    Assertion.invariant(op.isSystemComponent() && op.getOperatorAlgorithm() instanceof ProcessControlOperator);
    _doneTasks.add(() -> {
      // update the state in the work list
      SchedulerState state = new SchedulerState();
      state._pendingWork = Optional.of(new PendingSourceWork());
      _work.put(op, state);
      return false;
    });
  }

  @Override
  protected void scheduleSections() {
    boolean done = _doneTasks.poll().getAsBoolean();
    while (!done) {
      schedule();
      try {
        done = _doneTasks.take().getAsBoolean();
      } catch (InterruptedException ie) {
        Assertion.impossible(ie);
      }
    }
  }

  private boolean handleDoneTask(WorkTask workTask) {
    availableResources++;
//    System.out.println("Done: " + workTask._section);
    // unfinished work
    workTask._submittedWork.entrySet()
            .stream()
            .filter(e -> {
              SchedulerState state = _work.get(e.getKey().getTarget());
              Assertion.invariant(state._pendingWork.isPresent()); // at least the submitted work should be there!
              return state._pendingWork.get() instanceof PendingInputWork; // exclude source work
            })
            .forEach(e -> {
              SchedulerState state = _work.get(e.getKey().getTarget());
              PendingInputWork pWork = (PendingInputWork) state._pendingWork.get();
              if (e.getValue().isEmpty()) {
                if (pWork._pendingWork.get(e.getKey().getTargetPort()).isEmpty())
                  pWork._pendingWork.remove(e.getKey().getTargetPort());
                if (pWork._pendingWork.isEmpty()) // only the last one deletes the pending work
                  state._pendingWork = Optional.empty();
                else {
                  // nothing to be done
                }
              } else {
                InputPort targetPort = e.getKey().getTargetPort();
                if (!pWork._pendingWork.containsKey(targetPort)) pWork._pendingWork.put(targetPort, new ArrayDeque<>());
                ((PendingInputWork) state._pendingWork.get())._pendingWork.get(targetPort).addFirst(e.getValue());
              }
            });
    // results
    workTask._resultWork.entrySet()
            .stream()
            .filter(e -> !e.getValue().isEmpty())
            .forEach(e -> {
              SchedulerState state = _work.get(e.getKey().getTarget());
              if (state._pendingWork.isPresent())
                Assertion.invariant(state._pendingWork.get() instanceof PendingInputWork); // it has unfinished arcs!
              else
                state._pendingWork = Optional.of(new PendingInputWork());
              PendingInputWork pWork = (PendingInputWork) state._pendingWork.get();
              InputPort targetPort = e.getKey().getTargetPort();
              if (!pWork._pendingWork.containsKey(targetPort))
                pWork._pendingWork.put(targetPort, new ArrayDeque<>());

              WorkChunk v = e.getValue();

              ArrayDeque<WorkChunk> a = ((PendingInputWork) state._pendingWork.get())._pendingWork.get(targetPort);

//              while (!v.isEmpty()) {
//
//                final WorkChunk vn = new WorkChunk();
//
//                for (int i = 0; i < _desiredWorkSize && !v.isEmpty(); i++) {
//                  vn.add(v.poll());
//                }
//
//                if (!vn.isEmpty())
//                  a.addLast(vn);
//              }
//               a.addLast(v);

              // FIXME do this faster knowing that there is an array deque inside the work chunk.
              // implement a method inside work chunk to split itself efficiently.
              while (v.size() > _desiredWorkSize) {
                final WorkChunk vn = new WorkChunk(_desiredWorkSize);

                for (int i = 0; i < _desiredWorkSize; i++) {
                  vn.add(v.poll());
                }
                a.addLast(vn);
              }
              if (!v.isEmpty()) a.addLast(v);

            });

    // penalize sections that did not make any progress
    _runtimes.get(workTask._section)._opScheduler._scheduledWithoutProgress
            .forEach(o -> {
              // back-track the incoming arcs with no data to find the incoming arcs to the section needed
              // to make forward progress
              Set<Arc> needed = o.getOp().getGraphNodeInputConnections()
                      .stream()
                      .filter(a -> a.getImpl().isArcEmpty())
                      .map(a -> workTask._section.getOperators().contains(a.getSource()) ?
                              findIncomingSectionArcs(a.getSource(), workTask._section) :
                              Collections.singleton(a))
                      .flatMap(Set::stream)
                      // we have to check the current work available because the section might
                      // have run in parallel and produced new data again which is now already there!
                      .filter(a -> _work.containsKey(a.getTarget())) // op is not done yet
                      .filter(a -> { // check whether work is available
                        Optional<IPendingWork> pWork = _work.get(a.getTarget())._pendingWork;
                        return !pWork.isPresent() ||
                                (pWork.get() instanceof PendingInputWork &&
                                        (!((PendingInputWork) pWork.get())._pendingWork.containsKey(a.getTargetPort()) ||
                                                ((PendingInputWork) pWork.get())._pendingWork.get(a.getTargetPort()).isEmpty()));
                      })
                      .collect(Collectors.toSet());
              if(!needed.isEmpty()) _penalized.put(o.getOp(), needed);
            });

    // free sections that can make progress again
    workTask._resultWork.entrySet()
            .stream()
            .filter(e -> !e.getValue().isEmpty())
            .filter(e -> _penalized.values().stream().anyMatch(s -> s.contains(e.getKey())))
            .forEach(e -> {
              Optional<Map.Entry<OperatorCore, Set<Arc>>> maybeEntry
                      = _penalized.entrySet().stream().filter(e1 -> e1.getValue().contains(e.getKey())).findFirst();
              Assertion.invariant(maybeEntry.isPresent());
              // simple strategy: we did not know exactly which input was responsible such that
              // the op could not make progress. so if any of those that did not have data now has data, we give it another try.
              _penalized.remove(maybeEntry.get().getKey());
            });

    // FINISH_COMPUTATION section: some arcs are closed or some ops are done but not all -> sections are opaque to this
    // scheduler. that is, it behaves just like a big operator that contains state that can still be processed.
    workTask._section.getOperators().stream()
            .filter(o -> _runtimeState._opRuntimes.get(o).getOperatorState() ==
                    AbstractOperatorStateMachine.OperatorState.FINISHING_COMPUTATION ||
                    (!_work.get(o)._pendingWork.isPresent() &&
                            o.getGraphNodeInputConnections().stream().anyMatch(a -> !a.isQueueEmpty())))
            // potentially many ops on a section can have data still in their arcs. hence, they would be counted towards
            // the list of possible work in schedule(). however, it is enough to list the new first ones that still have
            // pending input data. once the section is scheduled, the op scheduler should find the most optimal schedule.
            .filter(o -> o.getGraphNodeInputConnections()
                    .stream()
                    .map(Arc::getSource)
                    .allMatch(uo -> !_work.containsKey(uo) || !_work.get(uo)._pendingWork.isPresent()))
            .forEach(o -> {
              SchedulerState state = _work.get(o);
              state._pendingWork = Optional.of(new PendingSourceWork());
            });
    // ops can be scheduled again
    workTask._section.getOperators().forEach(o -> _work.get(o).isExecuting = false);
    // remove done ops from the work list -> a section's ops can only be remove when all ops are done.
    // because a section is said to FINISH_COMPUTATION even when some of its ops are already done. scheduling even
    // these "done" ops will schedule the section and the op scheduler will find the necessary ops to be activated.
    if (workTask
            ._section
            .getOperators()
            .stream()
            .allMatch(o ->
                    _runtimeState._opRuntimes.get(o).getOperatorState() ==
                            AbstractOperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION)) {
      workTask._section.getOperators().forEach(_work::remove);
    }
    // done check
    if (_work.isEmpty()) {
      if (super.isSystemPhaseCompleted()) {
        return true;
      } else {
        _exception = new DeadlockException(_runtimeState._sectionGraph, Collections.emptyList(), _runtimeState._opRuntimes);
        return true;
      }
    } else {
      return false;
    }
  }

  private Set<Arc> findIncomingSectionArcs(OperatorCore op, AbstractSection s) {
    return op.getGraphNodeInputConnections().stream()
            .map(a -> s.getOperators().contains(a.getSource()) ?
                    findIncomingSectionArcs(a.getSource(), s) :
                    Collections.singleton(a))
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
  }

  private boolean handleException(Throwable e) {
    _exception = e instanceof WrappedRuntimeException ? e.getCause() : e;
    return true;
  }

  private void schedule() {
    // TODO allow to schedule as many operators as there are free threads (track the number of executing tasks.)
    // TODO keep this list as state and invalidate/update accordingly.
    Map<WorkBasedOperatorRuntime, Either> possibleWork = _work.entrySet()
            .stream()
            .filter(e -> !e.getValue().isExecuting)
            .filter(e -> e.getValue()._pendingWork.isPresent())
            .filter(e -> !_penalized.containsKey(e.getKey()))
            .filter(e -> e.getKey().getOutputPorts().isEmpty() ||
                    e.getKey().getOutputPorts()
                            .stream()
                            .flatMap(p -> p.getOutgoingArcs().stream())
                            .anyMatch(a -> {
                              SchedulerState state = _work.get(a.getTarget());
                              if (!state._pendingWork.isPresent()) {
                                return true;
                              } else if (state._pendingWork.get() instanceof PendingSourceWork) {
                                return true; // op on a section that can process data
                              } else {
                                  Assertion.invariant(state._pendingWork.get() instanceof PendingInputWork);
                                  PendingInputWork work = (PendingInputWork) state._pendingWork.get();
                                  return !work._pendingWork.containsKey(a.getTargetPort()) ||
                                          work._pendingWork.get(a.getTargetPort())
                                                  .stream()
                                                  .map(WorkChunk::size)
                                                  .reduce(0, Integer::sum)
                                                  < a.getArcBoundary();
                              }
                            }))
            .map(e -> new Tuple<>(_runtimeState._opRuntimes.get(e.getKey()), Either.of(e.getValue()._pendingWork.get())))
            .collect(Collectors.toMap(Tuple::first, Tuple::second));
//    assert !possibleWork.isEmpty();

    // FIXME implement such that all ops from scheduled section are removed
    for (; availableResources > 0 && !possibleWork.isEmpty(); availableResources--) {

      OperatorCore toSchedule = _schedAlgo.schedule(_runtimeState._sectionGraph.getAllOperators(), _desiredWorkSize, possibleWork);
      possibleWork.remove(_runtimeState._opRuntimes.get(toSchedule));
      TRACE.accept(toSchedule);
      Section s = _runtimeState._sectionGraph.findParentSection(toSchedule.getId());
      Map<Arc, WorkChunk> submittedWork = s.getIncomingArcs()
              .stream()
              .filter(a -> _work.get(a.getTarget())._pendingWork.isPresent()) // done ops
              .collect(Collectors.toMap(a -> a, a -> {
                IPendingWork workPending = _work.get(a.getTarget())._pendingWork.get();
                if (workPending instanceof PendingInputWork) {
                  PendingInputWork pWork = (PendingInputWork) workPending;
                  if (!pWork._pendingWork.containsKey(a.getTargetPort()))
                    pWork._pendingWork.put(a.getTargetPort(), new ArrayDeque<>());
                  ArrayDeque<WorkChunk> w = pWork._pendingWork.get(a.getTargetPort());
                  return w.isEmpty() ? new WorkChunk(0) : w.removeFirst();
                } else {
                  Assertion.invariant(workPending instanceof PendingSourceWork);
                  return new WorkChunk(0);
                }
              }));
      Map<Arc, WorkChunk> resultWork = s.getOutgoingArcs()
              .stream()
              .collect(Collectors.toMap(a -> a, a -> new WorkChunk(_desiredWorkSize)));
      s.getOperators().forEach(o -> _work.get(o).isExecuting = true);
//    System.out.println("scheduling: " + s);
      _executor.submit(new WorkTask(s, submittedWork, resultWork));
    }
  }

  @FunctionalInterface
  public interface ISchedulingAlgorithm {
    OperatorCore schedule(Set<OperatorCore> graph, int desiredWorkSize, Map<WorkBasedOperatorRuntime, Either> possibleWork);
  }

  public interface IPendingWork {
    Set<InputPort> getPortsWithData();
    int getWorkSize(InputPort inPort);
  }

  public static class Either {
    private Optional<PendingInputWork> _inputWork = Optional.empty();
    private Optional<PendingSourceWork> _srcWork = Optional.empty();

    private static Either of(IPendingWork w) {
      return w instanceof PendingInputWork ? of((PendingInputWork) w) : of((PendingSourceWork) w);
    }

    private static Either of(PendingInputWork w) {
      Either e = new Either();
      e._inputWork = Optional.of(w);
      return e;
    }

    private static Either of(PendingSourceWork w) {
      Either e = new Either();
      e._srcWork = Optional.of(w);
      return e;
    }

    public IPendingWork getPendingWork(){
      return _inputWork.isPresent() ? _inputWork.get() : _srcWork.get();
    }
  }

  private class SchedulerState {
    private Optional<IPendingWork> _pendingWork = Optional.empty();
    private boolean isExecuting = false;
  }

  private class PendingInputWork implements IPendingWork {
    private Map<InputPort, ArrayDeque<WorkChunk>> _pendingWork = new HashMap<>();

    @Override
    public Set<InputPort> getPortsWithData() {
      return _pendingWork.keySet();
    }

    @Override
    public int getWorkSize(InputPort inPort) {
      if (_pendingWork == null) throw new RuntimeException("IMPOSSIBLE!");
      if (_pendingWork.containsKey(inPort))
        return _pendingWork.get(inPort).stream().map(WorkChunk::getWorkSize).reduce(Integer::sum).get();
      else
        return 0;
    }
  }

  private class PendingSourceWork implements IPendingWork {
    @Override
    public Set<InputPort> getPortsWithData() {
      return Collections.emptySet();
    }

    @Override
    public int getWorkSize(InputPort inPort) {
      return 1;
    }
    /**
     * This is a marker that remains valid until the operator has finished its computation.
     */
  }

  private class WorkTask implements Callable<WorkTask> {

    private AbstractSection _section;
    private Map<Arc, WorkChunk> _submittedWork;
    private Map<Arc, WorkChunk> _resultWork;

    private WorkTask(AbstractSection section, Map<Arc, WorkChunk> submittedWork, Map<Arc, WorkChunk> resultWork) {
      _section = section;
      _submittedWork = submittedWork;
      _resultWork = resultWork;
    }

    @Override
    public WorkTask call() throws Exception {
      _submittedWork.entrySet()
              .stream()
              .forEach(e -> ((WorkBasedAsynchronousArc) e.getKey().getImpl()).assignWork(e.getValue()));
      _resultWork.entrySet()
              .stream()
              .forEach(e -> ((WorkBasedAsynchronousArc) e.getKey().getImpl()).assignResultChunk(e.getValue()));
      _runtimes.get(_section).call();
      _submittedWork.entrySet()
              .stream()
              .forEach(e -> ((WorkBasedAsynchronousArc) e.getKey().getImpl()).releaseWork());
      _resultWork.entrySet()
              .stream()
              .forEach(e -> ((WorkBasedAsynchronousArc) e.getKey().getImpl()).releaseResultChunk());
      return this;
    }
  }

  private class Executor extends ScheduledThreadPoolExecutor {

    public Executor(int corePoolSize) {
      super(corePoolSize);
    }

    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);

      // the Java Concurrency API is broken here. see comments and bug reports in internet.
      try {
        WorkTask task = (WorkTask) ((FutureTask) r).get();
        _doneTasks.add(() -> handleDoneTask(task));
      } catch (ExecutionException e) {
        // this is also totally stupid because what does the second parameter of this function do?!
        _doneTasks.add(() -> handleException(e.getCause()));
      } catch (InterruptedException ie){
        Assertion.impossible(ie);
      }
    }
  }
}
