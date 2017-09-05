/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.IDone;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorStateAccess;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;
import ohua.util.Tuple;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.exceptions.CompilationException.CAUSE;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

enum MatchType {
  EXPLICIT,
  IMPLICIT
}

/**
 * The algorithm responsible for matching input and output schemas of operators automatically.
 * It complements the functional operator.
 *
 * @author sertel
 */
public abstract class AbstractSchemaMatcher implements OperatorStateAccess, IDone, Stats.ILoggable {
  protected UserOperator _ohuaOp = null;
  protected List<String> _outputPortRefs = new ArrayList<>();
  protected Map<String, int[]> _explicitInputMatch = new HashMap<>();
  protected List<Object[]> _outPortControls = null;
  /**
   * Retrieves data from the input port and creates the arguments to be passed to the operator
   * function.
   */
  protected IFunctionalOperator _funcOp = null;
  /* new */
  protected List<String> _nInputPortRefs = new ArrayList<>();
  /**
   * Every array stores the formal slot indexes. The index of a formal slot in this array is used to retrieve the value from the array that arrives!
   */
  protected List<int[]> _explicitInputMatches = new ArrayList<>();
  private Supplier<Boolean> _canExecuteCall = () -> true;
  private DataRetrieval _dataRetrieval = new DataRetrieval();
  private CallArrayConstruction _callArrayConstruction = new CallArrayConstruction();
  private List<MatchType> _inputMatchType = new ArrayList<>();
  private Map<String, int[]> _explicitOutputMatch = new HashMap<>();
  private Tuple<Integer, Object>[] _arguments = new Tuple[0];

  private InputPortControl _contextInputControl = null;

  private Stats.IStatsCollector _dataRetrievalStats = null;
  private Stats.IStatsCollector _outputFrameworkStats = null;
  private Stats.IStatsCollector _callArrayConstructionStats = null;

  public AbstractSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
    _funcOp = functionalOperator;
    _ohuaOp = ohuaOperator;
  }

  /**
   * This method is currently needed in order to detect compoundness although only one reference
   * exists for this slot. Once a typed compilation is in place then this should just go away.
   * We then should detect compoundness by matching the type of the input reference (actual
   * schema) to the formal schema.
   *
   * @param argType
   * @return
   */
  protected static boolean isArrayArgument(Class<?> argType) {
    return (argType.isArray()
//            && argType.getComponentType().isAssignableFrom(Object.class)
    );
  }

  protected void setDataRetrieval(DataRetrieval dataRetrieval) {
    _dataRetrieval = _dataRetrieval.replace(dataRetrieval);
  }

  protected void setCallArrayConstruction(CallArrayConstruction cac) {
    _callArrayConstruction = cac;
  }

  public final void prepare() {

    // conditions first
    prepareContextInput();

    // get the controllers
    List<InputPortControl> tmp =
            _nInputPortRefs.stream().map(l -> _ohuaOp.getDataLayer().getInputPortController(l)).collect(Collectors.toList());

    // FIXME this code is duplicate in prepareContextInput() called above!
    // filter condition inputs
    List<Integer> conditionInputs = IntStream.range(0, _explicitInputMatches.size()).filter(
            i -> {
              int[] explicitMatch = _explicitInputMatches.get(i);
              return explicitMatch.length == 1 && explicitMatch[0] == -1;
            }).boxed().collect(Collectors.toList());
    conditionInputs.forEach(
            boxedConditionInput -> {
              int conditionInput = boxedConditionInput;
              _explicitInputMatches.remove(conditionInput);
              tmp.remove(conditionInput);
              _inputMatchType.remove(conditionInput);
              _nInputPortRefs.remove(conditionInput);
            });

    // prepare the runtime data structure
    List<ActualToFormal> actualsToFormals = IntStream.range(0, _explicitInputMatches.size()).boxed()
            .flatMap(inputPortIdx ->
            {
              int[] formalTargetSlots = _explicitInputMatches.get(inputPortIdx);
              return IntStream.range(0, formalTargetSlots.length)
                      .mapToObj(actualSlotIdx -> {
                        ActualToFormal f = new ActualToFormal();
                        f._formalSlotIdx = formalTargetSlots[actualSlotIdx];
                        f._actualDataIdx = actualSlotIdx;
                        f._formalType = getFormalType(_funcOp.getFormalArguments(), f._formalSlotIdx, _funcOp.isAlgoVarArgs());
                        f._matchType = _inputMatchType.get(inputPortIdx);
                        f._inputPort = _nInputPortRefs.get(inputPortIdx);
                        f._inControl = _ohuaOp.getDataLayer().getInputPortController(f._inputPort);
                        return f;
                      });
            }).collect(Collectors.toList());

    List<EnvArgToFormal> envArgsToFormals = Arrays.stream(_funcOp.getEnvironmentArguments())
            .map(envArg -> {
              EnvArgToFormal e = new EnvArgToFormal();
              e._formalSlotIdx = envArg._s;
              e._formalType = getFormalType(_funcOp.getFormalArguments(), e._formalSlotIdx, _funcOp.isAlgoVarArgs());
              e._envArg = envArg._t;
              e._actualType = envArg.getClass();
              return e;
            }).collect(Collectors.toList());

    prepareInputSide(actualsToFormals, envArgsToFormals);
    prepareOutputSide();

    initStatisticsCollection();
  }

  private void initStatisticsCollection() {
    _dataRetrievalStats = Stats.createStatsCollector();
    _outputFrameworkStats = Stats.createStatsCollector();
    _callArrayConstructionStats = Stats.createStatsCollector();
  }

  protected void prepareInputSide(List<ActualToFormal> actualsToFormals, List<EnvArgToFormal> envArgsToFormals) {
    // input port checks
    actualsToFormals.stream()
            .map(f -> f._inputPort)
            .distinct() // make sure we create the checker only once per input port (many slots retrieved from same input port)
            .map(i -> _dataRetrieval.createDataInputRetrieval(i, _ohuaOp))
            .forEach(ic ->
                    actualsToFormals.stream().filter(f -> f._inputPort.equals(ic._inPort.getPortName())).forEach(f -> f._checker = ic));

    // create one for the context input too!
    if (_contextInputControl != null)
      _dataRetrieval.createDataInputRetrieval(_contextInputControl.getPortName(), _ohuaOp);

    // call array construction
    _callArrayConstruction.prepare(actualsToFormals,
            envArgsToFormals,
            _funcOp.getFormalArguments(),
            _funcOp.isAlgoVarArgs(),
            _funcOp.useJavaVarArgsSemantics());
  }

  protected void prepareOutputSide() {
    _outPortControls = _outputPortRefs.stream().map(
            outPortRef ->
                    // FIXME make this a triple!
                    new Object[]{_ohuaOp.getDataLayer().getOutputPortController(outPortRef),
                            _explicitOutputMatch.get(outPortRef),
                            getOutputMatcher(outPortRef)}).collect(Collectors.toList());
  }

  protected OutputMatch.OutputMatcher getOutputMatcher(String outPortRef) {
    return StatefulFunction.getOutputMatcher(_funcOp.getFunctionType(),
            getReturnType(),
            findOutputMatchType(outPortRef));
  }

  protected Class<?> getReturnType() {
    return _funcOp.getReturnType();
  }


  private Class<?> getFormalType(Class<?>[] formals, int formalSlotIdx, boolean isVarArgs) {
    if (formalSlotIdx < formals.length - 1) {
      return formals[formalSlotIdx];
    } else if (formalSlotIdx == formals.length - 1) {
      // we return the type of the array here although it is possible that the actual data really is an array of
      // that type as the only parameter (Java var arg semantics).
      return isVarArgs ? formals[formals.length - 1].getComponentType() : formals[formalSlotIdx];
    } else {
      Assertion.invariant(formalSlotIdx >= formals.length && isVarArgs && formals[formals.length - 1].isArray());
      return formals[formals.length - 1].getComponentType();// type of the array
    }
  }

  /**
   * @return the _explicitInputMatch without the input for context result
   */
  private Map<String, int[]> prepareContextInput() {

    Map<String, int[]> filteredExplicitInputMatch = new HashMap<>();
    for (Map.Entry<String, int[]> entry : _explicitInputMatch.entrySet()) {
      String key = entry.getKey();
      int[] value = entry.getValue();
      if (value.length == 1 && value[0] == -1) {
        assert _contextInputControl == null;
        _contextInputControl =
                _ohuaOp.getDataLayer().getInputPortController(key);
        assert _contextInputControl != null;
        _canExecuteCall = () -> (boolean) _contextInputControl.getData();
      } else {
        filteredExplicitInputMatch.put(key, value);
      }
    }
    return filteredExplicitInputMatch;
  }

  protected final OutputMatch.MATCH_TYPE findOutputMatchType(String outputPortRef) {
    if (_explicitOutputMatch.get(outputPortRef) == null) return OutputMatch.MATCH_TYPE.ALL;
    else if (_explicitOutputMatch.get(outputPortRef).length == 1) return OutputMatch.MATCH_TYPE.SINGLE;
    else return OutputMatch.MATCH_TYPE.MULTIPLE;
  }

  public void cleanUp() {
    // nothing
  }

  public void logStats(Appendable resource) throws IOException {
    resource.append("\"sending\" : ");
    _outputFrameworkStats.log(resource);
    resource.append(", \"retrieval\" : ");
    _dataRetrievalStats.log(resource);
    resource.append(", \"call-array-const\" : ");
    _callArrayConstructionStats.log(resource);
  }

  public void compile(List<String> inputs, List<String> outputs) throws CompilationException {
    Map<String, int[]> opArgs = prepareExplicitInputs();
    validateActualToFormalAssignments(opArgs);
    validateFormalSlotAssignment();
  }

  private void validateFormalSlotAssignment() throws CompilationException {
    List<Integer> envSlots = Arrays.stream(_funcOp.getEnvironmentArguments()).map(Tuple::first).collect(Collectors.toList());
    Class<?>[] formalArgs = _funcOp.getFormalArguments();
    boolean isVarArgs = _funcOp.isAlgoVarArgs();
    List<Integer> flowInputSlots = _explicitInputMatches.stream().
            flatMapToInt(Arrays::stream).
            filter(l -> l > -1).
            sorted().
            boxed().
            collect(Collectors.toList());
    if (envSlots.size() > envSlots.stream().distinct().count()) {
      throw new CompilationException(CAUSE.ENVIRONMENT_ARGS_ASSIGNED_TO_SAME_SLOT, _funcOp.getFunctionName());
    }
    if (flowInputSlots.size() > flowInputSlots.stream().distinct().count()) {
      throw new CompilationException(CAUSE.ARGS_ASSIGNED_TO_SAME_SLOT, _funcOp.getFunctionName());
    }

    Set<Integer> intersect = new HashSet<>(envSlots);
    intersect.retainAll(flowInputSlots);
    if (!intersect.isEmpty()) {
      throw new CompilationException(CAUSE.ENV_ARG_ASSIGNED_TO_ALREADY_ASSIGEND_SLOT, _funcOp.getFunctionName());
    }

    if (!isVarArgs && formalArgs.length > envSlots.size() + flowInputSlots.size()
            || isVarArgs && formalArgs.length - 1 > envSlots.size() + flowInputSlots.size()) {
      throw new CompilationException(CAUSE.ARITY_TOO_FEW, _funcOp.getFunctionName());
    }

    if (!isVarArgs && formalArgs.length < envSlots.size() + flowInputSlots.size()) {
      throw new CompilationException(CAUSE.ARITY_TOO_MANY, _funcOp.getFunctionName());
    }
  }

  protected Map<String, int[]> prepareExplicitInputs() {

    Map<String, int[]> filteredExplicitInputMatch = new HashMap<>();
    for (Map.Entry<String, int[]> entry : _explicitInputMatch.entrySet()) {
      String key = entry.getKey();
      int[] value = entry.getValue();
      if (value.length == 1 && value[0] == -1) {
        // skip conditional inputs
      } else {
        filteredExplicitInputMatch.put(key, value);
      }
    }
    return filteredExplicitInputMatch;
  }

  private void
  validateActualToFormalAssignments(Map<String, int[]> explicitInputMatch) throws CompilationException {
    // formal slots (for flow args)
    int numFlowSlots = _funcOp.getFlowArgumentCount();

    // group by input slot (actual slots for flow args)
    Set<Integer> actualsSlots = new HashSet<>();
    for (Map.Entry<String, int[]> entry : explicitInputMatch.entrySet())
      for (int location : entry.getValue())
        actualsSlots.add(location);
    List<Integer> actualsSorted = new ArrayList<>(actualsSlots);
    Collections.sort(actualsSorted);

    if (numFlowSlots > actualsSorted.size()) {
      // FIXME this is impossible to tell because of implicit schema matching. currently, we
      // only have the information of how many output values a function has at runtime. later on
      // when compilation is in place we put this exception back in.
      // throw new CompilationException(CAUSE.ARITY_TOO_FEW, _funcOp.getFunctionName());
    } else {
      // only the last argument can be a compound one!
      // FIXME What is the last arg here?! I think we can't know that here! This needs fixing badly. This whole construction of the arguments needs to be fixed ASAP!
      if (numFlowSlots > 0
//              && isBasicArgument(numFlowSlots - 1)
              ) {
        // all is good
      } else {
        // no compound arg
        if (numFlowSlots == actualsSlots.size()) {
          // all good.
        } else {
          throw new CompilationException(CAUSE.ARITY_TOO_MANY, _funcOp.getFunctionName());
        }
      }
    }
  }

  public boolean hasConditionalInput() {
    for (Map.Entry<String, int[]> entry : _explicitInputMatch.entrySet()) {
      int[] value = entry.getValue();
      if (value.length == 1 && value[0] == -1)
        return true;
    }
    return false;
  }

  protected boolean isBasicArgument(int formalSchemaIndex) {
    Class<?> argType = _funcOp.getParameterType(formalSchemaIndex);
    return isArrayArgument(argType);
  }

  /**
   * Takes the results of the operator computation and emits them to the proper output ports.
   *
   * @param results
   */
  abstract public boolean matchOutputSchema(Object results);

  protected final Object matchOutputPort(OutputPortControl outPort, Object[] outRefPrep, Object results) {
    int[] indexes = (int[]) outRefPrep[1];
    OutputMatch.OutputMatcher<?> outMatcher = (OutputMatch.OutputMatcher<?>) outRefPrep[2];
//    System.out.println(_funcOp + " : " + outMatcher.getClass());
    return outMatcher.match(outPort, indexes, results);
  }

  protected final boolean sendResults(OutputPortControl outPort, Object finalResults) {
    _outputFrameworkStats.begin();
    outPort.newPacket();
    outPort.setData(LanguageDataFormat.LanguagePacketAccess.MATCH.name(), finalResults);
    boolean backOff = outPort.send();
    _outputFrameworkStats.end();
    return backOff;
  }

  abstract public Object[] matchInputSchema();

  protected boolean isExecuteCall() {
    return _canExecuteCall.get();
  }

  public boolean isCallDataAvailable() {
    _dataRetrievalStats.begin();
    boolean result = _dataRetrieval.isCallDataAvailable();
    _dataRetrievalStats.end();
    return result;
  }

  protected Object[] loadCallData() {
    // had to transform this for logging -> TODO: use lambda trick here? (or log inside CallArrayConstruction)
    _callArrayConstructionStats.begin();
    Object[] t = _callArrayConstruction.construct();
    _callArrayConstructionStats.end();
    return t;
  }

  public void registerOutputExplicitSchemaMatch(String outputPortRef, int[] explicitMatching) {
    _outputPortRefs.add(outputPortRef);
    if (explicitMatching[0] == -1) {
      // implicit match: all in
      // FIXME should explicitly store this information instead of holding it implicitly via !_explicitOutputMatch.contains(outputPortRef)
    } else {
      _explicitOutputMatch.put(outputPortRef, explicitMatching);
    }
  }

  public void registerInputExplicitSchemaMatch(String inputPortRef, int[] explicitMatching, int matchType) {
    _explicitInputMatch.put(inputPortRef, explicitMatching);

    _nInputPortRefs.add(inputPortRef);
    _explicitInputMatches.add(explicitMatching);
    _inputMatchType.add(matchType < 0 ? MatchType.IMPLICIT : MatchType.EXPLICIT);
  }

  @Override
  public Object getState() {
    return _dataRetrieval.getState();
  }

  @Override
  public void setState(Object state) {
    _dataRetrieval.setState(state);
  }

  /**
   * Transfer state that was set before preparation.
   *
   * @param old
   */
  public void transfer(AbstractSchemaMatcher old) {
    _outputPortRefs = old._outputPortRefs;
    _explicitOutputMatch = old._explicitOutputMatch;

    _explicitInputMatch = old._explicitInputMatch;
    _nInputPortRefs = old._nInputPortRefs;
    _explicitInputMatches = old._explicitInputMatches;
    _inputMatchType = old._inputMatchType;
    _dataRetrieval = old._dataRetrieval;
  }

  protected int[] getRegisteredInputs() {
    return _explicitInputMatch.values().stream().flatMapToInt(Arrays::stream).toArray();
  }

  public void addOutOfContextInput(String portName) {
    _dataRetrieval.addOutOfContextInput(portName);
  }

  @Override
  public boolean isComputationComplete() {
    return _dataRetrieval.isComputationComplete();
  }

  protected class ActualToFormal {
    /**
     * Index of the data in the array of actuals constructed to match the formal schema. <br/>
     * (Not that this is specified during algorithm construction and is not statically retrieved.
     * Therefore, this index may very well be beyond the defined formals in case of var args.)<br/>
     */
    int _formalSlotIdx;

    /**
     * Index of actual data in the data array that arrives at the input port.
     */
    int _actualDataIdx;

    /**
     * Formal type. In case of a reference into a var args array, this is the component type of the array.
     */
    Class<?> _formalType;

    /**
     * The match type for this actual.
     */
    MatchType _matchType;

    /**
     * The reference to the input port where the data arrives along with the control structures that take care of it.
     */
    String _inputPort;
    InputPortControl _inControl;
    DataRetrieval.InputDataChecker _checker;
  }

  protected class EnvArgToFormal {
    /**
     * Index of the data in the array of actuals constructed to match the formal schema. <br/>
     * (Not that this is specified during algorithm construction and is not statically retrieved.
     * Therefore, this index may very well be beyond the defined formals in case of var args.)<br/>
     */
    int _formalSlotIdx;

    /**
     * Actual data.
     */
    Object _envArg;

    /**
     * Formal type.
     */
    Class<?> _formalType;

    /**
     * Actual type.
     */
    Class<?> _actualType;
  }
}
