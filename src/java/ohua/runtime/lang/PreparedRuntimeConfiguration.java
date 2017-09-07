/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import clojure.java.api.Clojure;
import clojure.lang.*;
import ohua.runtime.engine.AbstractRuntime;
import ohua.runtime.engine.ConfigurationExtension;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.WorkBasedRuntime;
import ohua.runtime.engine.daapi.DataAccess;
import ohua.runtime.engine.daapi.DataFormat;
import ohua.runtime.engine.flowgraph.elements.ConcurrentArcQueue;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.scheduler.WorkBasedOperatorScheduler;
import ohua.runtime.engine.scheduler.WorkBasedTaskScheduler;
import ohua.runtime.engine.sections.AbstractSectionGraphBuilder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("serial")
public class PreparedRuntimeConfiguration extends RuntimeProcessConfiguration implements ConfigurationExtension {

  private CompileTimeView _cv = null;
  private RuntimeView _rv = null;
  private Map<String, Object> _compileTimeInfo = null;

  /*
   * computed
   */
  private List<OperatorID[]> _sectionRestrictions = new ArrayList<>();

  public PreparedRuntimeConfiguration(CompileTimeView cv, RuntimeView rv, Map<String, Object> compileTimeInfo) {
    _cv = cv;
    _rv = rv;
    _compileTimeInfo = compileTimeInfo;
  }

  @SuppressWarnings("unchecked")
  public void prepare(FlowGraph graph) {
    prepareSectionRestrictions();
    defineStrictCallSemantics();
  }

  private static List<Integer> testSchedule = Arrays.asList(
          121,96,106,108,106,117,99,104,108,103,117,99,99,112,118,97,98,100,101,105,102,115,109,114,116,113,111,107,110,99
  );

  private static class WrappingScheduler implements WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm {
    private Queue<Integer> schedule;
    private WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm innerAlgorithm;

    public WrappingScheduler(Queue<Integer> schedule, WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm innerAlgorithm) {
      this.schedule = schedule;
      this.innerAlgorithm = innerAlgorithm;
    }

    @Override
    public WorkBasedOperatorRuntime schedule(Set<WorkBasedOperatorRuntime> graph, List<WorkBasedOperatorRuntime> readyOps) {
      Integer top = schedule.poll();
      if (top == null) {
        return innerAlgorithm.schedule(graph, readyOps);
      } else {
        return readyOps.stream().filter(op -> op.getOp().getId().getIDInt() == top).findFirst().orElseThrow( () -> new RuntimeException("Could not find operator " + top));
      }
    }
  }

  private static class OpScheduler implements WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm {
    private int lastOpId = -1;
    private int opCounter = 0;
    private static int MIN_PRIORITY = -2;
//    private static final IFn require = Clojure.var("clojure.core/require");
//    private static final IFn spit = Clojure.var("clojure.core", "spit");
//    private static final IFn renderfn;
    private int graphDumpSeqNum = 0;
    private boolean wasUnsuccessful = false;
    private static final boolean REPORT_FAILURE = false;


    // TODO I do understand that it is good to have such a thing here but having the Clojure dependency always in the engine/scheduler is not so cool.
    // FIXME duplicate code: TaskScheduler
//    static {
//      require.invoke(Clojure.read("[ohua.util.visual]"));
//      renderfn = Clojure.var("ohua.util.visual", "render-runtime-graph");
//    }

    private void dumpGraph(Set graph, int chosen, String filename) {
//      if (graph == null) throw new RuntimeException("No graph!");
//      renderfn.invoke(graph, chosen, filename);
    }

    private void dumpGraph(Set graph, String filename) {
//      if (graph == null) throw new RuntimeException("No graph!");
//      renderfn.invoke(graph, filename);
    }

    private void dumpReasoning(List<WorkBasedOperatorRuntime> possibleWork, String filename) {
//      spit.invoke(
//              filename,
//              possibleWork.stream().map(rt ->
//                      rt.getOp().getId().getIDInt()
//                      + "," + rt.getGraphPriority()
//                      + ",["
//                      + rt.getOp().getInputPorts()
//                              .stream()
//                              .map(p ->
//                                      "[" + p.getIncomingArc().getSource().getId().getIDInt() + "-> :" + (p.hasSeenLastPacket() || !p.getIncomingArc().isQueueEmpty()) + "]")
//                              .collect(Collectors.joining(",")) + "],["
//                      + rt.getOp().getOutputPorts()
//                              .stream()
//                              .map(OutputPort::getOutgoingArcs)
//                              .flatMap(List::stream)
//                              .map(a ->
//                                      " ->" + a.getTarget().getId().getIDInt() + " : " + (a.getLoadEstimate() * 2 < a.getArcBoundary()))
//                              .collect(Collectors.joining(","))
//                      + "]"
//              ).collect(Collectors.joining("\n")));
    }

    private WorkBasedOperatorRuntime scheduleInternal(Set<WorkBasedOperatorRuntime> graph, List<WorkBasedOperatorRuntime> possibleWork) {

      WorkBasedOperatorRuntime candidate = null;
      int candidatePriority = MIN_PRIORITY;

      for (WorkBasedOperatorRuntime w : possibleWork) {
        int wprio = w.getGraphPriority();
        if (
                wprio > candidatePriority
                && w.getOp().getInputPorts()
                        .stream()
                        .allMatch(p ->
                                p.hasSeenLastPacket() || !p.getIncomingArc().isQueueEmpty())
                && w.getOp().getOutputPorts()
                        .stream()
                        .map(OutputPort::getOutgoingArcs)
                        .flatMap(List::stream)
                        // perhaps at some point we can make it so the queue does not have to be empty but be not very full
                        .allMatch(a -> a.getLoadEstimate() < a.getArcBoundary())
                ) {
          candidate = w;
          candidatePriority = wprio;
        }
      }

      // in case we did not find a suitable candidate
      if (candidate == null) {
        if (REPORT_FAILURE) {
          System.out.println("No suitable ideal candidate found, scheduling last operator");
          dumpGraph(graph, "debug-dump/scheduler-graphs/unsuccessful-" + graphDumpSeqNum + ".dot");
          dumpReasoning(possibleWork, "debug-dump/scheduler-graphs/unsuccessful-" + graphDumpSeqNum++ + ".reasoning");
          wasUnsuccessful = true;
        }
        return possibleWork.get(possibleWork.size() - 1);
      } else {
        if (wasUnsuccessful) {
          if (REPORT_FAILURE) dumpGraph(graph, candidate.getOp().getId().getIDInt() ,"debug-dump/scheduler-graphs/successful-" + graphDumpSeqNum++ + ".dot");
        }
      }

      return candidate;
    }

    @Override
    public WorkBasedOperatorRuntime schedule(Set<WorkBasedOperatorRuntime> graph, List<WorkBasedOperatorRuntime> possibleWork) {
      final WorkBasedOperatorRuntime op = scheduleInternal(graph, possibleWork);
      final int opIdInt = op.getOp().getId().getIDInt();

      if (REPORT_FAILURE) {
        if (opIdInt == lastOpId) {
          opCounter++;
          final List<InputPort> ports = op.getOp().getInputPorts();
          if (opCounter == 10) {
            System.out.println("Repeat scheduling ready operator " + op.getOp().getID() + " with ports who have seen last packet " + ports.stream().filter(InputPort::hasSeenLastPacket).count() + "/" + ports.size() + " and ports with non-empty queue " + ports.stream().filter(i -> !i.getIncomingArc().isQueueEmpty()).count() + "/" + ports.size());
            System.out.println("Arc Impl: " + ports.get(0).getIncomingArc().getImpl().getClass());
            System.exit(2);
          }
          System.out.println("Repeat schedule " + op.getOp().getOperatorName() + " with load estimates: " + ports.stream().map(InputPort::getIncomingArc).map(Arc::getLoadEstimate).map(Object::toString).collect(Collectors.joining(", ")));
        } else {
          opCounter = 1;
          lastOpId = opIdInt;
//        System.out.println("Load estimate of op" + op.getOp().getOperatorName() + ": " + op.getOp().getInputPorts().stream().map(InputPort::getIncomingArc).map(Arc::getLoadEstimate).map(Object::toString).collect(Collectors.joining(", ")));
        }
      }
      return op;
    }
  }

  private static class TaskScheduler implements WorkBasedTaskScheduler.ISchedulingAlgorithm {
    private int lastOpId = -1;
    private int opCounter = 0;
    private static int MIN_PRIORITY = -2;
//    private static final IFn require = Clojure.var("clojure.core/require");
//    private static final IFn spit = Clojure.var("clojure.core", "spit");
//    private static final IFn renderfn;
    private int graphDumpSeqNum = 0;
    private boolean wasUnsuccessful = false;

    private static final boolean REPORT_FAILIURE = false;

// TODO I do understand that it is good to have such a thing here but having the Clojure dependency always in the engine/scheduler is not so cool.
//    static {
//      require.invoke(Clojure.read("[ohua.util.visual]"));
//      renderfn = Clojure.var("ohua.util.visual", "render-op-graph");
//    }
//
    private void dumpGraph(Set graph, int chosen, String filename) {
//      if (graph == null) throw new RuntimeException("No graph!");
//      renderfn.invoke(graph, chosen, filename);
    }
//
    private void dumpGraph(Set graph, String filename) {
//      if (graph == null) throw new RuntimeException("No graph!");
//      renderfn.invoke(graph, filename);
    }
//
    private void dumpReasoning(final int desiredWorkSize, Map<WorkBasedOperatorRuntime, WorkBasedTaskScheduler.Either> possibleWork, String filename) {
//      spit.invoke(
//              filename,
//              possibleWork.keySet().stream().map(rt ->
//                      rt.getOp().getId().getIDInt()
//                              + "," + rt.getGraphPriority()
//                              + ",["
//                              + rt.getOp().getInputPorts()
//                              .stream()
//                              .map(p ->
//                                      "[" + p.getIncomingArc().getSource().getId().getIDInt() + "-> :" + (p.hasSeenLastPacket() || (p.getIncomingArc().getLoadEstimate() >= desiredWorkSize)) + "]")
//                              .collect(Collectors.joining(",")) + "],["
//                              + rt.getOp().getOutputPorts()
//                              .stream()
//                              .map(OutputPort::getOutgoingArcs)
//                              .flatMap(List::stream)
//                              .map(a ->
//                                      " ->" + a.getTarget().getId().getIDInt() + " : " + (a.getRemainingCapacityEstimate() >= desiredWorkSize))
//                              .collect(Collectors.joining(","))
//                              + "]"
//              ).collect(Collectors.joining("\n")));
    }

    private OperatorCore scheduleInternal(Set<OperatorCore> graph, final int desiredWorkSize, Map<WorkBasedOperatorRuntime, WorkBasedTaskScheduler.Either> possibleWork) {

      OperatorCore idealCandidate = null;
      int idealCandidatePriority = MIN_PRIORITY;

      OperatorCore candidate = null;
      int candidatePriority = MIN_PRIORITY;

      Map<OperatorCore, WorkBasedOperatorRuntime> m = possibleWork.keySet().stream().collect(Collectors.toMap(AbstractOperatorRuntime::getOp, Function.identity()));


      for (Map.Entry<WorkBasedOperatorRuntime, WorkBasedTaskScheduler.Either> e : possibleWork.entrySet()) {
        WorkBasedOperatorRuntime wr = e.getKey();
        WorkBasedTaskScheduler.Either eth = e.getValue();
        OperatorCore w = wr.getOp();

        WorkBasedTaskScheduler.IPendingWork pw = eth.getPendingWork();

        int wprio = wr.getGraphPriority();
        if (
                wprio > idealCandidatePriority
                && w.getInputPorts()
                        .stream()
                        .allMatch(p ->
                                p.hasSeenLastPacket() || pw.getWorkSize(p) >= desiredWorkSize)
                && w.getOutputPorts()
                        .stream()
                        .map(OutputPort::getOutgoingArcs)
                        .flatMap(List::stream)
                        .allMatch(a -> Optional.ofNullable(m.get(a.getTarget())).flatMap(rt -> Optional.ofNullable(possibleWork.get(rt))).map(either -> either.getPendingWork().getWorkSize(a.getTargetPort())).orElse(0) < a.getArcBoundary() - desiredWorkSize)
                ) {
          idealCandidate = w;
          idealCandidatePriority = wprio;
        } else if (
                idealCandidate == null
                && wprio > candidatePriority
                && w.getInputPorts()
                        .stream()
                        .allMatch(p ->
                                p.hasSeenLastPacket() || !(pw.getWorkSize(p) == 0))
                && w.getOutputPorts()
                        .stream()
                        .map(OutputPort::getOutgoingArcs)
                        .flatMap(List::stream)
                        // perhaps at some point we can make it so the queue does not have to be empty but be not very full
                        .allMatch(a -> a.getRemainingCapacityEstimate() * 2 >= desiredWorkSize)
                ) {
          candidate = w;
          candidatePriority = wprio;
        }
      }

      // in case we did not find a suitable idealCandidate
      if (idealCandidate == null) {
        if (candidate == null) {
          if (REPORT_FAILIURE) {
            System.out.println("No suitable candidate found, using default scheduling algorithm");
            wasUnsuccessful = true;
            dumpGraph(graph, "debug-dump/scheduler-graphs/unsuccessful-" + graphDumpSeqNum + ".dot");
            dumpReasoning(desiredWorkSize, possibleWork, "debug-dump/scheduler-graphs/unsuccessful-" + graphDumpSeqNum++ + ".reasoning");
          }
          return WorkBasedTaskScheduler.DEFAULT_SCHEDULING_ALGO.schedule(graph, desiredWorkSize, possibleWork);
        } else {
          if (REPORT_FAILIURE) {
            System.out.println("No suitable ideal candidate found");
            dumpGraph(graph, "debug-dump/scheduler-graphs/no-ideal-" + graphDumpSeqNum + ".dot");
            dumpReasoning(desiredWorkSize, possibleWork, "debug-dump/scheduler-graphs/unsuccessful-" + graphDumpSeqNum++ + ".reasoning");
          }
          return candidate;
        }
      } else {
        if (wasUnsuccessful) {
          if (REPORT_FAILIURE) dumpGraph(graph, "debug-dump/scheduler-graphs/successful-" + graphDumpSeqNum++ + ".dot");
        }
      }

      return idealCandidate;
    }

    @Override
    public OperatorCore schedule(Set<OperatorCore> graph, final int desiredWorkSize, Map<WorkBasedOperatorRuntime, WorkBasedTaskScheduler.Either> possibleWork) {
      final OperatorCore op = scheduleInternal(graph, desiredWorkSize, possibleWork);
      final int opIdInt = op.getId().getIDInt();

      if (REPORT_FAILIURE) {
        if (opIdInt == lastOpId) {
          opCounter++;
          final List<InputPort> ports = op.getInputPorts();
          if (opCounter == 10) {
            System.out.println("Repeat scheduling ready operator " + op.getID() + " with ports who have seen last packet " + ports.stream().filter(InputPort::hasSeenLastPacket).count() + "/" + ports.size() + " and ports with non-empty queue " + ports.stream().filter(i -> !i.getIncomingArc().isQueueEmpty()).count() + "/" + ports.size());
            System.out.println("Arc Impl: " + ports.get(0).getIncomingArc().getImpl().getClass());
            System.exit(2);
          }
          System.out.println("Repeat schedule " + op.getOperatorName() + " with load estimates: " + ports.stream().map(InputPort::getIncomingArc).map(Arc::getLoadEstimate).map(Object::toString).collect(Collectors.joining(", ")));
        } else {
          opCounter = 1;
          lastOpId = opIdInt;
//        System.out.println("Load estimate of op" + op.getOp().getOperatorName() + ": " + op.getOp().getInputPorts().stream().map(InputPort::getIncomingArc).map(Arc::getLoadEstimate).map(Object::toString).collect(Collectors.joining(", ")));
        }
      }
      return op;
    }
  }

  private void defineStrictCallSemantics() {
    // for the section scheduler
    WorkBasedTaskScheduler.ISchedulingAlgorithm taskSchedulerAlgo = new TaskScheduler();

    super._properties.put(WorkBasedTaskScheduler.SCHEDULING_ALGO, taskSchedulerAlgo);

    // for the operator scheduler
    WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm opSchedulerAlgo = new OpScheduler();
    super._properties.put(
            WorkBasedOperatorScheduler.SCHEDULING_ALGO,
            opSchedulerAlgo
    );
  }

  public AbstractRuntime getRuntime() {
    return new WorkBasedRuntime(this);
  }

  private void prepareSectionRestrictions() {
//    if (_compileTimeInfo.containsKey(FlowGraphCompiler.SHARED_VARIABLES_INFO)) {
//      List<OperatorID[]> securityRestrictions =
//              new SecurityRestrictionAnalysis((Map<String, List<int[]>>) _compileTimeInfo.get(FlowGraphCompiler.SHARED_VARIABLES_INFO)).defineRestrictions(_cv,
//                      _rv);
//      _sectionRestrictions.addAll(securityRestrictions);
//    }
  }

  public AbstractSectionGraphBuilder getSectionStrategy() {
    if (_properties.getProperty("section-strategy") == null) _properties.setProperty("section-strategy",
            "ohua.runtime.lang.RestrictionBasedSectionMapping");

    if (_sectionRestrictions.isEmpty()) return super.getSectionStrategy();
    else return new RestrictedSectionMapping(this, super.getSectionStrategy(), _sectionRestrictions);
  }

  public DataAccess getDataAccess(AbstractOperatorRuntime op, DataFormat dataFormat) {
    assert dataFormat == null;
    return new LanguageDataAccess(op, dataFormat);
  }

  public DataFormat getDataFormat() {
    return null;
  }

  public Class<? extends Deque> getInterSectionQueueImpl() {
    ConcurrentArcQueue.BatchedConcurrentLinkedDeque.BATCH_SIZE = getOrDefault("batching-concurrent-queue.batch-size", 10);
    assert ConcurrentArcQueue.BatchedConcurrentLinkedDeque.BATCH_SIZE < getInterSectionArcBoundary();
    return ConcurrentArcQueue.BatchedConcurrentLinkedDeque.class;
  }

  protected <T> T getOrDefault(String key, T defaultValue) {
    return (T) _properties.getOrDefault(key, defaultValue);
  }

}
