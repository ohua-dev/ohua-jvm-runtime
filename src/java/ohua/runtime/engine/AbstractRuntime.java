/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.EndOfStreamPacketHandler;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.handers.UserGraphEntranceEOSMarkerHandler;
import ohua.runtime.engine.operators.system.UserGraphEntranceOperator;
import ohua.runtime.engine.scheduler.AbstractScheduler;
import ohua.runtime.engine.scheduler.IRuntime;
import ohua.runtime.engine.sections.AbstractSectionGraphBuilder;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.sections.SingleSectionMapper;
import ohua.runtime.engine.utils.GraphVisualizer;
import ohua.util.Tuple;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by sertel on 1/26/17.
 */
public abstract class AbstractRuntime<T extends AbstractOperatorRuntime> implements IRuntime {

  protected RuntimeProcessConfiguration _runtimeConfiguration;
  protected AbstractScheduler _scheduler;

  private Runnable _tearDown;

  protected AbstractRuntime(RuntimeProcessConfiguration config){
    _runtimeConfiguration = config;
  }

  public Tuple<OperatorCore, OperatorCore> initializeRuntime(FlowGraph graph) {
    RuntimeState<T> runtimeState = new RuntimeState<>();
    runtimeState._sectionGraph = initializeGraph(graph);
    GraphVisualizer.printSectionGraph(runtimeState._sectionGraph);
    GraphVisualizer.printFlowGraph(graph);
    runtimeState._opRuntimes = initializeOperatorRuntimes(runtimeState._sectionGraph);
    initializeOperators(runtimeState._opRuntimes.values().stream().collect(Collectors.toSet()));
    initializeScheduling(runtimeState);

    Set<AbstractOperatorRuntime> opRuntimes = runtimeState._opRuntimes.values().stream().collect(Collectors.toSet());
    FlowGraphControl.prepareAndEnterComputationState(opRuntimes);
    _tearDown = () -> FlowGraphControl.tearDownAndFinishComputation(opRuntimes);
    return new Tuple<>(runtimeState._sectionGraph
            .findOperator(EngineMetaComponentCreator.MetaOperator.PROCESS_CONTROL.opName()),
            runtimeState._sectionGraph.getUserGraphExitSection().getOperator());
  }

  private SectionGraph initializeGraph(FlowGraph graph) {
    AbstractSectionGraphBuilder sectionConstruction = null;

    // FIXME remove this configuration parameter: thread-count == 0 means single-threaded execution.
    switch(_runtimeConfiguration.getExecutionMode())
    {
      case MULTI_THREADED:
        sectionConstruction = _runtimeConfiguration.getSectionStrategy();
        break;
      case SINGLE_THREADED:
        sectionConstruction = getIslandToSectionMapperForSingleThreading();
        _runtimeConfiguration.setCoreThreadPoolSize(1);
        _runtimeConfiguration.setMaxThreadPoolSize(1);
        break;
    }

    // split the graph
    SectionGraph sectionGraph = sectionConstruction.build(graph);
    attachSystemComponents(graph, sectionGraph);
    initializeArcs(sectionGraph);
    return sectionGraph;
  }

  private Map<OperatorCore, T> initializeOperatorRuntimes(SectionGraph sectionGraph){
    return sectionGraph.getEntireOperatorWorld()
            .stream()
            .map(this::createOperatorRuntime)
            .peek(SystemOperatorAdapter::setRuntime)
            .collect(Collectors.toMap(T::getOp, r -> r));
  }

  abstract protected T createOperatorRuntime(OperatorCore op);

  private void initializeScheduling(RuntimeState state) {
    _scheduler = createScheduler();
    _scheduler.initialize(state, _runtimeConfiguration);
  }

  abstract protected AbstractScheduler createScheduler();

  protected void initializeArcs(SectionGraph graph) {
    int arcBoundary = _runtimeConfiguration.getInnerSectionArcBoundary();
    graph.getAllArcs().stream().forEach(a -> {
      a.setImpl(createArcImpl(a));
      a.setArcBoundary(arcBoundary);
    });
  }

  abstract protected AbstractArcImpl createArcImpl(Arc arc);

  private void attachSystemComponents(FlowGraph graph, SectionGraph sectionGraph) {
    new EngineMetaComponentCreator(graph, sectionGraph).attachMetaComponents();
    attachAdditionalSystemComponents(sectionGraph);
  }

  protected void attachAdditionalSystemComponents(SectionGraph graph) {
    // FIXME disabled. needs to be rewritten!
//    if(_runtimeConfiguration.enableOnlineConfigurationSupport()) {
//      ConfigurationGraphRewrite.attachSystemComponents(graph);
//    }
  }

  protected AbstractSectionGraphBuilder getIslandToSectionMapperForSingleThreading() {
    return new SingleSectionMapper(_runtimeConfiguration);
  }

  private void initializeOperators(Set<T> operators) {
    operators.stream().forEach(op -> {
      OhuaOperator ohuaOp = new OhuaOperator(op);
      initializeOperator(ohuaOp, op.getOp());

      // data access layer
      op.getOp().setDataLayer(_runtimeConfiguration.getDataAccess(op, _runtimeConfiguration.getDataFormat()));
    });
  }

  // FIXME refactoring: 2 steps: 1. create visitors 2. create and register handlers
  private void initializeOperator(OhuaOperator op, OperatorCore core) {
    EndOfStreamPacketHandler endHandler = retrieveEndOfStreamPacketHandler(op);
    FlowGraphControl.performPortHandlerSetup(op, core, endHandler);
  }

  protected EndOfStreamPacketHandler retrieveEndOfStreamPacketHandler(OhuaOperator op) {
    if(op.getUserOperator() instanceof UserGraphEntranceOperator) {
      return new UserGraphEntranceEOSMarkerHandler(op);
    }
    else {
      return new EndOfStreamPacketHandler(op);
    }
  }

  @Override
  public void launch(Set<OperatorCore> ops) {
    _scheduler.launch(ops);
  }

  @Override
  public void start(SystemPhaseType systemPhase) {
    _scheduler.start(systemPhase);
  }

  @Override
  public void teardown() {
    _tearDown.run();
    _scheduler.teardown();
  }

  @Override
  public void onDone(Consumer<Optional<Throwable>> onDone) {
    _scheduler.onDone(onDone);
  }

  @Override
  public void activate(OperatorCore op) {
    _scheduler.activate(op);
  }

  public static class RuntimeState<S extends AbstractOperatorRuntime>{
    public SectionGraph _sectionGraph;
    public Map<OperatorCore, S> _opRuntimes;

    private RuntimeState(){}
  }

}
