/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.util.Tuple;

import java.util.*;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sertel on 1/24/17.
 */
public class WorkBasedOperatorScheduler extends AbstractOperatorScheduler<WorkBasedOperatorRuntime> {

  public static Consumer<WorkBasedOperatorRuntime> TRACE = op -> {};
  protected Set<WorkBasedOperatorRuntime> _scheduledWithoutProgress = new HashSet<>();
  private Set<WorkBasedOperatorRuntime> _doneForThisCycle = new HashSet<>();

  public static final String SCHEDULING_ALGO = "operator.scheduler.algo";
  public static IOperatorSchedulingAlgorithm DEFAULT_SCHEDULING_ALGO =  (gr, ops) -> ops.get(0);
  private IOperatorSchedulingAlgorithm _schedAlgo = DEFAULT_SCHEDULING_ALGO;

  // I've made this into a field. If the graph is changed we have to update this thing. But this is much more efficient.
  private Map<OperatorCore, WorkBasedOperatorRuntime> _coreMap;

  public WorkBasedOperatorScheduler(Set<WorkBasedOperatorRuntime> graph, RuntimeProcessConfiguration config) {
    super(graph);
    config.aquirePropertiesAccess(props -> {
      Object algo = props.get(SCHEDULING_ALGO);
      if(algo != null){
        if(algo instanceof IOperatorSchedulingAlgorithm){
          _schedAlgo = (IOperatorSchedulingAlgorithm) algo;
          // NOTICE we can remove this again if you like, but I need the graph for debugging right now
        }else{
          throw new IllegalArgumentException("Submitted scheduling algo does not implement " + IOperatorSchedulingAlgorithm.class.getName());
        }
      } else {
        // nothing -> default is already assigned
      }
    });
  }

  @Override
  protected void init() {
    _coreMap = _graph.stream().collect(Collectors.toMap(WorkBasedOperatorRuntime::getOp,Function.identity(), (a, b) -> a));
    super.init();
    initArcs();
  }

  private void initArcs() {
    _graph.stream()
            .flatMap(o -> o.getOp().getGraphNodeOutputConnections().stream())
            .filter(a -> _coreMap.containsKey(a.getTarget())
                    // This should be more efficient. Map lookup instead of traversing the graph again

//                    _graph.stream().anyMatch(r -> r.getOp() == a.getTarget()) // FIXME same problem again! either the graph can answer this question or the implementation of the arc that this section sees must answer this question!
            )
            .forEach(a -> {
              WorkBasedAsynchronousArc wa = (WorkBasedAsynchronousArc) a.getImpl();
              // assign work chunks already because this scheduler just leaves the work inside the arcs.
              wa.assignWork(new WorkChunk(a.getArcBoundary()));
              wa.assignResultChunk(new WorkChunk(a.getArcBoundary()));
            });
  }

  protected void prepareOpForExecution(WorkBasedOperatorRuntime op) {
    super.prepareOpForExecution(op);

    // input side
    op.getOp().getInputPorts()
            .stream()
            .map(InputPort::getIncomingArc)
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> wa.assignWork(wa.releaseWork().memorize()));

    // the output side was already prepared in handleDoneExecution or is ready by default
    op.getOp().getOutputPorts()
            .stream()
            .map(OutputPort::getOutgoingArcs)
            .flatMap(List::stream)
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> wa.assignResultChunk(wa.releaseResultChunk().memorize()));
//    System.out.println("Op scheduled: " + op.getOp().getOperatorName());
    TRACE.accept(op);
  }

  protected Optional<WorkBasedOperatorRuntime> schedule() {
      // I'm hoping this will be more efficient than the filter operations
      Set <WorkBasedOperatorRuntime> s0 = new HashSet<>(_graph);
      s0.removeAll(_scheduledWithoutProgress);
      s0.removeAll(_doneForThisCycle);

    List<WorkBasedOperatorRuntime> readyOps = s0.stream()
            .filter(o -> {
              Stream<InputPort> s = o.getOp().getInputPorts().stream();
              return o.getOperatorState() == AbstractOperatorStateMachine.OperatorState.FINISHING_COMPUTATION ||
                      o.getOp().getNumGraphNodeInputs() == 0 || // <-- this is only here to kick of the process controller
                      s.anyMatch(i -> !i.getIncomingArc().isQueueEmpty());
            })
            .filter(o -> o.getOp().getOutputPorts().isEmpty() ||
                    o.getOp().getOutputPorts()
                            .stream()
                            .flatMap(p -> p.getOutgoingArcs().stream())
                            .anyMatch(a -> a.getLoadEstimate() < a.getArcBoundary()))
            .sorted((o1, o2) -> o2.getGraphPriority() - o1.getGraphPriority())
            .collect(Collectors.toList());
    return readyOps.isEmpty() ? Optional.empty() : Optional.of(_schedAlgo.schedule(_graph, readyOps));
  }

  protected void handleDoneExecution(WorkBasedOperatorRuntime op) {
    assureProgress(op);

    if (op.getOperatorState() == AbstractOperatorStateMachine.OperatorState.WAITING_FOR_COMPUTATION)
      _doneForThisCycle.add(op);

    transferDataAndArcState(op.getOp());
    releaseProgressableOps(op.getOp());
  }

  /**
   * Checks both input and output side to understand whether the operator made any progress.
   * If so then reschedule this guy otherwise just don't.
   *
   * @param op
   */
  private void assureProgress(WorkBasedOperatorRuntime op) {
    BooleanSupplier opDequeued = () -> op.getOp().getInputPorts()
            .stream()
            .map(InputPort::getIncomingArc)
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .anyMatch(wa -> {
              WorkChunk work = wa.releaseWork();
              wa.assignWork(work);
              return work.getMemoizedSize() > work.size();
            });
    BooleanSupplier opEnqueued = () -> op.getOp().getOutputPorts()
            .stream()
            .map(OutputPort::getOutgoingArcs)
            .flatMap(List::stream)
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .anyMatch(wa -> {
              WorkChunk result = wa.releaseResultChunk();
              wa.assignResultChunk(result);
              return result.getMemoizedSize() < result.size();
            });

    boolean opMadeProgress = opDequeued.getAsBoolean() || opEnqueued.getAsBoolean();
    if (!opMadeProgress) {
      // memoize the input side to understand when this guy can make progress again
      op.getOp().getInputPorts()
              .stream()
              .map(InputPort::getIncomingArc)
              .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
              .forEach(wa -> wa.assignWork(wa.releaseWork().memorize()));
      _scheduledWithoutProgress.add(op);
    }
  }

  private void transferDataAndArcState(OperatorCore op) {
    // transfer data to downstream
    op.getOutputPorts()
            .stream()
            .flatMap(p -> p.getOutgoingArcs().stream())
            .filter(a ->
                    _coreMap.containsKey(a.getTarget())
                    //_graph.stream().anyMatch(r -> r.getOp() == a.getTarget())
            )

            // FIXME same problem again! either the graph can answer this question or the implementation of the arc that this section sees must answer this question!
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> {
              // source side
              WorkChunk results = wa.releaseResultChunk();

              // target side
              WorkChunk existingWork = wa.releaseWork();
              existingWork.addAll(results);
              wa.assignWork(existingWork);

              // reuse work chunks
              int workSize = results.getWorkSize();
              results.clear();
              wa.assignResultChunk(results.setLowerWorkBound(workSize));
            });
    // reset the state tracked in the input arcs
    op.getInputPorts()
            .stream()
            .map(InputPort::getIncomingArc)
            .filter(a ->
                    //_graph.stream().anyMatch(r -> r.getOp() == a.getSource())
                    _coreMap.containsKey(a.getSource())
            )
            // FIXME same problem again! either the graph can answer this question or the implementation of the arc that this section sees must answer this question!
            .map(a -> (WorkBasedAsynchronousArc) a.getImpl())
            .forEach(wa -> {
              WorkChunk work = wa.releaseWork();
              wa.assignWork(work);
              WorkChunk result = wa.releaseResultChunk();
              wa.assignResultChunk(result);

              // transfer the amount still inside the work part backwards
              result.setLowerWorkBound(work.size());
            });
  }

  private void releaseProgressableOps(OperatorCore op) {
    // TBD should I do this also for the output side because the op might not have been able to make
    // progress because all its outgoing arcs were full. (I probably should not schedule this ops in the first place!)
    op.getGraphNodeOutputConnections()
            .stream()
            .map((Arc rt) ->
                    Optional.ofNullable(_coreMap.get(rt.getTarget())).map(Tuple.partialTuple1(rt))
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
//            .filter(a ->
//                    _graph.stream().anyMatch(r -> r.getOp() == a.getTarget())
//            )

            // FIXME same problem again! either the graph can answer this question or the implementation of the arc that this section sees must answer this question!
//            .map(a -> new Tuple<>(a, _graph.stream().filter(r -> r.getOp() == a.getTarget()).findFirst().get())
//            )
            // FIXME same problem again! either the graph can answer this question or the implementation of the arc that this section sees must answer this question!
            .filter(t -> _scheduledWithoutProgress.contains(t.second()))
            .filter(t -> {
              WorkBasedAsynchronousArc wa = (WorkBasedAsynchronousArc) t.first().getImpl();
              WorkChunk work = wa.releaseWork();
              wa.assignWork(work);
              // I could also test whether all arcs are not 0 but this is more fine-grained and detects even
              // when a single arc already carries more data. which might be what the op needed to make progress.
              return work.getMemoizedSize() < work.size();
            })
            .forEach(t -> _scheduledWithoutProgress.remove(t.second()));
  }

  protected void beforeExecutionStep() {
    _scheduledWithoutProgress.clear();
    _doneForThisCycle.clear();
  }

  protected void afterExecutionStep() {
    // nothing yet
  }


  @FunctionalInterface
  public interface IOperatorSchedulingAlgorithm {
    WorkBasedOperatorRuntime schedule(Set<WorkBasedOperatorRuntime> graph, List<WorkBasedOperatorRuntime> readyOps);
  }

}
