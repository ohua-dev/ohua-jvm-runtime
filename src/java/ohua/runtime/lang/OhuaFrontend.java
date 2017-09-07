/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.link.JavaBackendProvider;
import ohua.runtime.lang.operator.IFunctionalOperator;
import ohua.runtime.lang.operator.SFNLinker;
import ohua.util.Tuple;
import ohua.runtime.engine.DataFlowProcess;
import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.DataFlowComposition;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.flowgraph.elements.operator.Arc.ArcType;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.exceptions.CompilationException.CAUSE;
import ohua.link.Linker;
import org.codehaus.janino.Java;

import java.util.*;

/**
 * We will interact via this class with the compilers to be supported. It is meant to wrap calls
 * to find operators and their implementations and build the flow graph. Finally, it also needs
 * to provide code hooks into the runtime.
 *
 * @author sertel
 *
 */
public abstract class OhuaFrontend extends DataFlowComposition {
  protected DataFlowProcess _process = new DataFlowProcess();
  private CompileTimeInfo _compileInfo = new CompileTimeInfo();
  private Map<Integer, List<int[]>> _dependencies = new LinkedHashMap<>();

  public OhuaFrontend() { }

    public static final OperatorCore findOperator(FlowGraph graph, String type) {
        List<OperatorCore> ops = graph.getOperators(type + "-[0-9]*");
        if (ops.size() != 1) {
            throw new RuntimeException("Name ambiguity for operator: " + type);
        } else {
            return ops.get(0);
        }
  }

  /**
   *
   * @param source - id of the source function
   * @param sourcePos - position of the value in the source function's output
   * @param target - id of the target function
   * @param targetPos - position of the value in the target function's input
   */
  public void registerDependency(int source, int sourcePos, int target, int targetPos) {
    registerDependency(source, sourcePos, target, targetPos, 0);
  }

  public void registerDependency(int source, int sourcePos, int target, int targetPos, int isFeedback) {
    if(!_dependencies.containsKey(source)) _dependencies.put(source, new ArrayList<int[]>());
    _dependencies.get(source).add(new int[] { sourcePos,
                                              target,
                                              targetPos,
                                              isFeedback });
  }

  protected final void resolveDependencies() {
    for(Map.Entry<Integer, List<int[]>> entry : _dependencies.entrySet()) {

      // group by target
      Map<Integer, List<int[]>> grpByTarget = new HashMap<>();
      for(int[] deps : entry.getValue()) {
        if(!grpByTarget.containsKey(deps[1])) grpByTarget.put(deps[1], new ArrayList<int[]>());
        grpByTarget.get(deps[1]).add(deps);
      }

      // create arcs
      for(Map.Entry<Integer, List<int[]>> e : grpByTarget.entrySet()) {
        int[] explicitSourceMatching = new int[e.getValue().size()];
        int[] explicitTargetMatching = new int[e.getValue().size()];
        for(int i = 0; i < explicitSourceMatching.length; i++)
          explicitSourceMatching[i] = e.getValue().get(i)[0];
        for(int i = 0; i < explicitTargetMatching.length; i++)
          explicitTargetMatching[i] = e.getValue().get(i)[2];
        Assertion.invariant(!e.getValue().isEmpty());
        createArc(entry.getKey(),
                  e.getKey(),
                  explicitSourceMatching,
                  explicitTargetMatching,
                  e.getValue().get(0)[3]);
      }
    }

    _dependencies.clear();
//    GraphVisualizer.printFlowGraph(_process.getGraph());
  }

  protected IOperatorFactory operatorFactory() {
    return SFNLinker.getInstance();
  }

  /**
   * Construct a new operator of a given type.
   *
   * @param id
   * @throws OperatorLoadingException
   * @throws CompilationException
   */
  public void createOperator(String type, int id) throws OperatorLoadingException, CompilationException {
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) {
      System.out.println("Received request to create operator. >> type: " + type + " id: " + id);
    }

    try {
      OperatorCore op = super.loadOperator(type, _process.getGraph());
      // TODO it might be useful to assign a name to an operator when it is defined in a let
      // binding
      op.setOperatorName(constructOperatorName(type, id));
    }
    catch(OperatorLoadingException e) {
      e.printStackTrace();
      throw e;
    }

    // System.out.println("Operator creation finished successfully.");
  }

  /**
   * Construct a new arc between two operators.
   *
   * @param source
   * @param target
   */
  public int[] createArc(int source, int target) {
    return createArc(source, target, 0);
  }

  public int[] createArc(int source, int target, int arcType) {
    if(RuntimeProcessConfiguration.LOGGING_ENABLED) {
      System.out.println("Received request to create arc. >> source: " + source + " target: " + target);
    }
    Arc arc = super.createArc();
    arc.setType(ArcType.values()[arcType]);
    OperatorCore sourceOp = findOperator(_process.getGraph(), source);
    OperatorCore targetOp = findOperator(_process.getGraph(), target);
    OutputPort outPort = new OutputPort(sourceOp);
    outPort.setPortName("out-" + sourceOp.getOutputPorts().size());
    InputPort inPort = new InputPort(targetOp);
    inPort.setPortName("in-" + targetOp.getInputPorts().size());
    sourceOp.addOutputPort(outPort);
    targetOp.addInputPort(inPort);
    arc.setSourcePort(outPort);
    arc.setTargetPort(inPort);
    _process.getGraph().addArc(arc);
    return new int[] { sourceOp.getNumOutputPorts() - 1,
                       targetOp.getNumInputPorts() - 1 };
  }

  public void createArc(int source, int target, int[] explicitMatching) {
    createArc(source, target, explicitMatching, new int[0], 0);
  }

  private int getMatchType(int[] explicitTargetMatching, int definedMatchType){
    return explicitTargetMatching.length == 1 ? -1 : definedMatchType;
  }


  /**
   * Construct a new arc with explicit schema matching.
   *
   * @param source
   * @param target
   */
  public void createArc(int source, int target, int[] explicitSourceMatching, int[] explicitTargetMatching,
                        int isFeedback)
  {
    int[] portIdxes = createArc(source, target, isFeedback);

    OperatorCore sourceOp = findOperator(_process.getGraph(), source);
    Assertion.invariant(sourceOp.getOperatorAlgorithm() instanceof IFunctionalOperator);
    ((IFunctionalOperator) sourceOp.getOperatorAlgorithm()).setExplicitOutputSchemaMatch(portIdxes[0],
                                                                                         explicitSourceMatching);
    Integer id = new Integer(source);
    if(!_compileInfo._outputSchema.containsKey(id)) _compileInfo._outputSchema.put(id, new ArrayList<int[]>());
    _compileInfo._outputSchema.get(id).add(explicitSourceMatching);

    OperatorCore targetOp = findOperator(_process.getGraph(), target);
    Assertion.invariant(targetOp.getOperatorAlgorithm() instanceof IFunctionalOperator);
    ((IFunctionalOperator) targetOp.getOperatorAlgorithm()).setExplicitInputSchemaMatch(portIdxes[1],
                                                                                        explicitTargetMatching,
                                                                                        getMatchType(explicitTargetMatching, explicitSourceMatching[0]));
    registerInputSchema(target, explicitTargetMatching, explicitSourceMatching[0]);
  }

  protected void registerInputSchema(int target, int[] explicitTargetMatching, int matchType) {
    Integer id = new Integer(target);
    if(!_compileInfo._inputSchema.containsKey(id)) _compileInfo._inputSchema.put(id, new ArrayList<Object[]>());
    _compileInfo._inputSchema.get(id).add(new Object[] { explicitTargetMatching,
                                                         getMatchType(explicitTargetMatching, matchType) });
  }

  public void setArguments(int operator, Tuple<Integer, Object>[] arguments) throws CompilationException {
    OperatorCore op = findOperator(_process.getGraph(), operator);
    AbstractOperatorAlgorithm algo = op.getOperatorAlgorithm();
    if(!(algo instanceof IFunctionalOperator)) {
      throw new CompilationException(CAUSE.UNSUPPORTED_OPERATOR);
    } else {
      ((IFunctionalOperator) algo).setArguments(arguments);
      _compileInfo._arguments.put(new Integer(operator), arguments);
    }
  }

  @Override
  public DataFlowProcess load() throws Exception {
    return _process;
  }

  public CompileTimeView getCompileTimeView() {
    final CompileTimeInfo compileInfo = _compileInfo;
    return new CompileTimeView() {
      @Override
      public List<Object[]> getInputSchema(int opID) {
        return compileInfo._inputSchema.get(new Integer(opID));
      }

      @Override
      public List<int[]> getOutputSchema(int opID) {
        return compileInfo._outputSchema.get(new Integer(opID));
      }

      @Override
      public Tuple<Integer, Object>[] getArguments(int opID) {
        if(compileInfo._arguments.containsKey(new Integer(opID))) return compileInfo._arguments.get(new Integer(opID));
        else return new Tuple[0];
      }

      @Override
      public int extractIDFromRef(String operatorRef) {
        return Integer.parseInt(deconstructOperatorName(operatorRef)[1]);
      }
    };
  }

  protected final String constructOperatorName(String type, int id) {
    return type + "-" + id;
  }

  protected final String[] deconstructOperatorName(String operatorName) {
    String[] result = new String[2];
    result[0] = operatorName.substring(0, operatorName.lastIndexOf("-"));
    result[1] = operatorName.substring(operatorName.lastIndexOf("-") + 1);
    return result;
  }

  protected final OperatorCore findOperator(FlowGraph graph, int id) {
    List<OperatorCore> ops = graph.getOperators(".*-" + id);
    if(ops.size() > 1) {
      throw new RuntimeException("Name ambiguity for operator: " + id);
    } else if(ops.size() < 1) {
      throw new RuntimeException("Operator not found: " + id);
    } else {
      return ops.get(0);
    }
  }

    private class CompileTimeInfo {
        // op-id -> input schema (one per port)
        Map<Integer, List<Object[]>> _inputSchema = new HashMap<>();
        // op-id -> output schema (one per port)
        Map<Integer, List<int[]>> _outputSchema = new HashMap<>();
        // op-id -> arguments
        Map<Integer, Tuple<Integer, Object>[]> _arguments = new HashMap<>();
    }

}
