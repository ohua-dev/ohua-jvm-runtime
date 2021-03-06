/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.*;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.WorkBasedOperatorRuntime;
import ohua.runtime.engine.operators.ConsumerOperator;
import ohua.runtime.engine.operators.GeneratorOperator;
import ohua.runtime.engine.sections.ConfigurableSectionMapper;
import ohua.runtime.test.AbstractFlowTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class testTaskScheduler extends AbstractFlowTestCase {
  private Consumer<HashMap<String, Object>> _registerWorkBasedRuntime;

  private int threadPoolSize = 1;
  private int desiredWorkSize = -1;

  protected ProcessRunner createProcessRunner(Supplier<DataFlowProcess> process) {
    ProcessRunner runner = super.createProcessRunner(process);
    prepareWorkbasedRuntime(runner);
    return runner;
  }

  protected ProcessRunner createProcessRunner(Supplier<DataFlowProcess> process, RuntimeProcessConfiguration config) {
    ProcessRunner runner = super.createProcessRunner(process, config);
    prepareWorkbasedRuntime(runner);
    return runner;
  }

  private void prepareWorkbasedRuntime(ProcessRunner runner){
    _registerWorkBasedRuntime = (newProps) -> runner.getConfig()
            .aquirePropertiesAccess(props -> {
              props.put(WorkBasedTaskScheduler.DESIRED_WORK_SIZE, desiredWorkSize == -1 ? runner.getConfig().getInterSectionArcBoundary() : desiredWorkSize);
              props.put(RuntimeProcessConfiguration.BuiltinProperties.RUNTIME.getKey(), WorkBasedRuntime.class);
              props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, threadPoolSize);
              props.putAll(newProps);

            });
  }

  @Test(timeout = 10000)
  public void testEnginePhases() throws Throwable {
    RuntimeProcessConfiguration config = new RuntimeProcessConfiguration();
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE, RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE, 20);
    config._properties.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY, 25);
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"), config);
    _registerWorkBasedRuntime.accept(new HashMap<>());
    manager.initializeProcess();
    manager.awaitSystemPhaseCompletion();
    performInitPhaseAssertions(manager);
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is above what is being processed.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowWithoutOperatorScheduling() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(100));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 1000);
    _registerWorkBasedRuntime.accept(props);
    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is below of what is being processed.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowWithOperatorScheduling() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(100));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is below of what is being processed and we run in a multi-threaded mode
   * but 1 thread only.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowWithTaskSchedulingSequential() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(100));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put("core-thread-pool-size", 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is below of what is being processed and we run in a multi-threaded mode
   * with 2 threads.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowWithTaskSchedulingParallel() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(100));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 2);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * More complex flow with non-deterministic merge and split.<br/>
   * 2 threads
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testComplexNDMergeFlowWithoutOperatorScheduling() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 1000);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }


  @Test(timeout = 10000)
  public void testComplexNDMergeFlowWithOperatorScheduling() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    List<String> trace = new LinkedList<>();
    WorkBasedOperatorScheduler.TRACE = op -> trace.add(op.getOp().getOperatorName());

    try {
      runFlowNoAssert(manager);

      Assert.assertEquals(100,
              ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
      Assert.assertEquals(100,
              ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
    } catch (Throwable t) {
      System.out.println(trace);
    }
  }

  @Test(timeout = 10000)
  public void testComplexNDMergeFlowWithTaskSchedulingSequential() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 10000)
  public void testComplexNDMergeFlowWithTaskSchedulingParallel() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 2);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * More complex flow with deterministic merge and split.<br/>
   * 2 threads
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithoutOperatorScheduling() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 1000);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithOperatorScheduling() throws Throwable {
    int dataAmount = 50;
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Left-DataGenerator").getOperatorAlgorithm())
            .getProperties().setAmountToGenerate(dataAmount);
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Right-DataGenerator").getOperatorAlgorithm())
            .getProperties().setAmountToGenerate(dataAmount);
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Right-DataGenerator").getOperatorAlgorithm())
            .getProperties().setStartOffset(dataAmount);
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    List<String> trace = new LinkedList<>();
    WorkBasedOperatorScheduler.TRACE = op -> trace.add(op.getOp().getOperatorName());

    try {
      runFlowNoAssert(manager);

      Assert.assertEquals(dataAmount,
              ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
      Assert.assertEquals(dataAmount,
              ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
    } catch (Throwable t) {
      System.out.println(trace);
      throw t;
    }
  }

  /**
   * This schedule leads the DataSplit into a state FINISHING_COMPUTATION where one outgoing arc is full. The test
   * checks that the code in WorkBasedOperatorRuntime.hasFinishedComputation() works properly and detects the
   * finished computation. Otherwise this would fail with an AssertionError because it would penalize the DataSplit
   * for not making any progress, but it would rest in this list forever because releasing an operator out of this
   * list can only be done via delivering new data on the input side.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithOperatorSchedulingNonDeterministicBug() throws Throwable {
    int dataAmount = 50;
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Left-DataGenerator").getOperatorAlgorithm())
            .getProperties().setAmountToGenerate(dataAmount);
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Right-DataGenerator").getOperatorAlgorithm())
            .getProperties().setAmountToGenerate(dataAmount);
    ((GeneratorOperator) manager.getProcess().getGraph().getOperator("Right-DataGenerator").getOperatorAlgorithm())
            .getProperties().setStartOffset(dataAmount);
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);

    List<String> scheduleToBug = Arrays.asList(
            "ProcessController", "Entrance", "Right-DataGenerator", "Right-Input-Logger", "Right-DataGenerator",
            "Left-DataGenerator", "Left-Input-Logger", "Left-DataGenerator", "DataMerge", "Left-Input-Logger",
            "Logger", "Right-Input-Logger", "Right-DataGenerator", "Left-DataGenerator", "DataMerge",
            "Left-Input-Logger", "Right-Input-Logger", "DataSplit", "Right-Output-Logger", "Right-Consumer",
            "Logger", "DataMerge", "Left-Input-Logger", "Right-Input-Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Logger", "DataMerge", "DataSplit", "Logger", "DataMerge", "DataSplit", "Logger",
            "DataMerge", "DataSplit", "Logger", "DataMerge", "DataSplit", "Logger", "DataMerge", "DataSplit",
            "Logger", "DataMerge", "DataSplit", "Logger", "DataMerge", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Logger", "DataMerge", "DataSplit", "Logger", "DataSplit", "Logger",
            "DataSplit", "Logger", "DataSplit", "Logger", "DataSplit", "Logger", "DataSplit", "Logger", "DataSplit",
            "Logger", "DataSplit", "Logger", "DataSplit", "Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Logger", "DataSplit", "DataSplit", "DataSplit", "DataSplit", "DataSplit",
            "DataSplit", "DataSplit", "DataSplit", "DataSplit", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "DataSplit", "Left-Output-Logger", "Left-Consumer", "Left-Output-Logger",
            "Left-Consumer", "Left-Output-Logger", "Left-Consumer"//, "Exit"
    );
    ArrayList<String> schedule = new ArrayList<>(scheduleToBug);
    props.put(WorkBasedOperatorScheduler.SCHEDULING_ALGO, (WorkBasedOperatorScheduler.IOperatorSchedulingAlgorithm) (graph, ops) -> {
      if (schedule.isEmpty()) {
        return WorkBasedOperatorScheduler.DEFAULT_SCHEDULING_ALGO.schedule(graph, ops);
      } else {
        String currOp = schedule.remove(0);
        return ops.stream().filter(o -> o.getOp().getOperatorName().equals(currOp)).findFirst().get();
      }
    });

    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(dataAmount,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(dataAmount,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithTaskSchedulingSequentialNonDeterministicBug() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);

    String[] scheduleToBug = new String[]{"ProcessController", "Entrance", "Left-DataGenerator",
            "Left-Input-Logger", "Left-DataGenerator", "DataMerge", "Left-Input-Logger", "Left-DataGenerator",
            "Logger", "DataSplit", "Right-Output-Logger", "Right-Consumer", "Exit", "Left-Output-Logger",
            "Left-Consumer", "Exit", "Right-DataGenerator", "Right-Input-Logger", "DataMerge", "Logger",
            "DataSplit", "Right-Output-Logger", "Right-Consumer", "DataMerge", "Left-Input-Logger",
            "Left-DataGenerator", "Left-Output-Logger", "Left-Consumer", "Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Left-Output-Logger", "Left-Consumer", "Right-DataGenerator",
            "Right-Input-Logger", "DataMerge", "Logger", "DataSplit", "Right-Output-Logger", "Right-Consumer",
            "DataMerge", "Left-Output-Logger", "Left-Consumer", "Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Left-Output-Logger", "Left-Consumer", "Right-DataGenerator", "Right-Input-Logger",
            "Right-DataGenerator"};
    List<String> schedule = new ArrayList<>(Arrays.asList(scheduleToBug));
    props.put(WorkBasedTaskScheduler.SCHEDULING_ALGO, (WorkBasedTaskScheduler.ISchedulingAlgorithm) (gr, workSize, possibleWork) -> {
      if (schedule.isEmpty()) {
        return WorkBasedTaskScheduler.DEFAULT_SCHEDULING_ALGO.schedule(gr, workSize, possibleWork);
      } else {
        String currOp = schedule.remove(0);
        return possibleWork.keySet().stream().map(WorkBasedOperatorRuntime::getOp).filter(k -> k.getOperatorName().equals(currOp)).findFirst().orElseThrow(() -> new IllegalStateException("Op not found: " + currOp));
      }
    });

    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 50000)
  public void testComplexDMergeFlowWithTaskSchedulingSequential() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithTaskSchedulingParallel() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 2);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test(timeout = 10000)
  public void testComplexNDMergeFlowWithTaskSchedulingSequentialLastAlgo() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    props.put(WorkBasedTaskScheduler.SCHEDULING_ALGO,
            (WorkBasedTaskScheduler.ISchedulingAlgorithm) (gr, ws, possibleWork) -> {
              List<OperatorCore> l = possibleWork.keySet().stream().map(AbstractOperatorRuntime::getOp).collect(Collectors.toList());
              Collections.reverse(l);
              return l.get(0);
            });
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }


  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithTaskSchedulingSequentialLastAlgo() throws Throwable {
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    props.put(WorkBasedTaskScheduler.SCHEDULING_ALGO,
            (WorkBasedTaskScheduler.ISchedulingAlgorithm) (gr, ws, possibleWork) -> {
              List<OperatorCore> l = possibleWork.keySet().stream().map(AbstractOperatorRuntime::getOp).collect(Collectors.toList());
              Collections.reverse(l);
              return l.get(0);
            });
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * A test case where the operator scheduler backs out to the task scheduler with a section where some ops are done and some are not.
   * this test will make sure that the task scheduler can activate these half-done sections properly.
   * flow: the section to back out would need input from another op on another section. flow with two generators where one generator and the
   * succeeding merge are put onto the same section. need to make sure that this section runs first! (han
   *
   * @throws Throwable
   */
  @Test
  public void testComplexSectionMappingNDMergeFlow() throws Throwable {
    runComplexSectionMappingFlow(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("NonDeterministicMerge"));
  }

  @Test
  public void testComplexSectionMappingDMergeFlow() throws Throwable {
    runComplexSectionMappingFlow(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
  }

  private void runComplexSectionMappingFlow(FlowGraph graph) throws Throwable {
    AbstractProcessManager manager = loadProcess(graph);
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 1000);
    ConfigurableSectionMapper sectionMapper = new ConfigurableSectionMapper(null);
    List<List<String>> mapping = new LinkedList<>();
    mapping.add(Arrays.asList("Left-DataGenerator", "Left-Input-Logger", "Right-Input-Logger",
            "DataMerge", "Logger", "DataSplit", "Left-Output-Logger", "Left-Consumer", "Right-Output-Logger",
            "Right-Consumer"));
    mapping.add(Arrays.asList("Right-DataGenerator"));
    sectionMapper.setSectionsMapping(mapping);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.SECTION_STRATEGY.getKey(), sectionMapper);
    props.put(WorkBasedTaskScheduler.SCHEDULING_ALGO,
            (WorkBasedTaskScheduler.ISchedulingAlgorithm) (gr, ws, possibleWork) ->
                    // give lowest priority to the Right-DataGenerator
                    possibleWork.size() == 1 ?
                            WorkBasedTaskScheduler.DEFAULT_SCHEDULING_ALGO.schedule(gr, ws, possibleWork) :
                            possibleWork.keySet()
                                    .stream()
                                    .map(AbstractOperatorRuntime::getOp)
                                    .filter(o -> !o.getOperatorName().equals("Right-DataGenerator"))
                                    .findFirst().get());
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  @Test
  public void testLargePoolComplexSectionMappingDMergeFlow() throws Throwable {

    int threadPoolSize = 3;

    this.threadPoolSize = threadPoolSize;
    runComplexSectionMappingFlow(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
  }


  @Test
  public void testSmallWorkComplexSectionMappingDMergeFlow() throws Throwable {

    this.desiredWorkSize = 10;
    runComplexSectionMappingFlow(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
  }

  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithTaskSchedulingSequentialNonDeterministicBugAndLowWorkSize() throws Throwable {
    desiredWorkSize = 5;
    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 1);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);

    String[] scheduleToBug = new String[]{"ProcessController", "Entrance", "Left-DataGenerator",
            "Left-Input-Logger", "Left-DataGenerator", "DataMerge", "Left-Input-Logger", "Left-DataGenerator",
            "Logger", "DataSplit", "Right-Output-Logger", "Right-Consumer", "Exit", "Left-Output-Logger",
            "Left-Consumer", "Exit", "Right-DataGenerator", "Right-Input-Logger", "DataMerge", "Logger",
            "DataSplit", "Right-Output-Logger", "Right-Consumer", "DataMerge", "Left-Input-Logger",
            "Left-DataGenerator", "Left-Output-Logger", "Left-Consumer", "Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Left-Output-Logger", "Left-Consumer", "Right-DataGenerator",
            "Right-Input-Logger", "DataMerge", "Logger", "DataSplit", "Right-Output-Logger", "Right-Consumer",
            "DataMerge", "Left-Output-Logger", "Left-Consumer", "Logger", "DataSplit", "Right-Output-Logger",
            "Right-Consumer", "Left-Output-Logger", "Left-Consumer", "Right-DataGenerator", "Right-Input-Logger",
            "Right-DataGenerator"};
    List<String> schedule = new ArrayList<>(Arrays.asList(scheduleToBug));
    props.put(WorkBasedTaskScheduler.SCHEDULING_ALGO, (WorkBasedTaskScheduler.ISchedulingAlgorithm) (gr, workSize, possibleWork) -> {
      if (schedule.isEmpty()) {
        return WorkBasedTaskScheduler.DEFAULT_SCHEDULING_ALGO.schedule(gr, workSize, possibleWork);
      } else {
        String currOp = schedule.remove(0);
        return possibleWork.keySet().stream().map(WorkBasedOperatorRuntime::getOp).filter(k -> k.getOperatorName().equals(currOp)).findFirst().orElseThrow(() -> new IllegalStateException("Op not found: " + currOp));
      }
    });

  }

  @Test(timeout = 10000)
  public void testComplexDMergeFlowWithTaskSchedulingParallelAndLowWorkSize() throws Throwable {
    desiredWorkSize = 5;

    AbstractProcessManager manager =
            loadProcess(testEnginePhasesMT.oneOpOneSectionComplexCorrectnessFlow2("DeterministicMerge"));
    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey(), 2);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 25);
    _registerWorkBasedRuntime.accept(props);

    runFlowNoAssert(manager);

    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Left-Consumer").getOperatorAlgorithm()).getSeenPackets());
    Assert.assertEquals(100,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Right-Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is above what is being processed.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowDataAmountLowerThanWorkSizeST() throws Throwable {
    int dataAmount = 5;
    desiredWorkSize = 10;
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(dataAmount));

    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.SINGLE_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 50);
    _registerWorkBasedRuntime.accept(props);
    runFlowNoAssert(manager);

    Assert.assertEquals(dataAmount,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

  /**
   * Generator -> DatabaseWriter
   * Arc boundary is above what is being processed.
   *
   * @throws Throwable
   */
  @Test(timeout = 10000)
  public void testSimpleFlowDataAmountLowerThanWorkSizeMT() throws Throwable {
    int dataAmount = 5;
    desiredWorkSize = 10;
    AbstractProcessManager manager =
            loadProcess(testNotificationBasedRuntime.oneOpOneSectionBuilderSimpleCorrectnessFlow(dataAmount));

    HashMap<String, Object> props = new HashMap<>();
    props.put(RuntimeProcessConfiguration.BuiltinProperties.EXECUTION_MODE.getKey(), RuntimeProcessConfiguration.Parallelism.MULTI_THREADED);
    props.put(RuntimeProcessConfiguration.BuiltinProperties.ARC_BOUNDARY.getKey(), 50);
    _registerWorkBasedRuntime.accept(props);
    runFlowNoAssert(manager);

    Assert.assertEquals(dataAmount,
            ((ConsumerOperator) manager.getProcess().getGraph().getOperator("Consumer").getOperatorAlgorithm()).getSeenPackets());
  }

}
