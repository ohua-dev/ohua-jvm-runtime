/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.IDone;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;
import ohua.util.Tuple;
import ohua.lang.defsfn;
import ohua.runtime.exceptions.CompilationException;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;

public abstract class AbstractFunctionalOperator extends UserOperator implements IFunctionalOperator, IDone, Stats.ILoggable {
  private Object _functionObject = null;

  private Tuple<Integer, Object>[] _arguments = new Tuple[0];
  private StatefulFunction _algorithm = null;

  private AbstractSchemaMatcher _schemaMatcher = new FunctionalSchemaMatching.SchemaMatcher(this, this);
  private FunctionExecution _funcExec = new FunctionExecution();

  private Stats.IStatsCollector _frameworkStats = null;
  private Stats.IStatsCollector _fnStats = null;
  private Stats.IStatsCollector _outputSideStats = null;

  protected AbstractFunctionalOperator() {
    // limit visibility of this class
  }

  private void initStatisticsCollection() {
    _frameworkStats = Stats.createFrameworkStatsCollector();
    _fnStats = Stats.createFunctionStatsCollector();
    _outputSideStats = Stats.createStatsCollector();
  }

  protected abstract AbstractSchemaMatcher getSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator);

  /**
   * At compile-time this is an array of types while at runtime this an array of actual values!
   *
   * @param arguments
   */
  public final void setArguments(Tuple<Integer, Object>[] arguments) {
    if (arguments == null) return;
    else {
      _arguments = arguments;
    }
  }

  public Tuple<Integer, Object>[] getEnvironmentArguments() {
    return _arguments;
  }

  public boolean isAlgoVarArgs() {
    return _algorithm.getMethod().isVarArgs();
  }

  /**
   * Compiles the operator against the provided arguments.
   *
   * @return
   */
  public void compile(boolean typeSensitive) throws CompilationException {
    try {
      _algorithm = detectAlgorithm();
      if (!isTypeCompatible(_algorithm.getMethod(), _arguments, typeSensitive)) {
        // TODO ones this was enabled again, please also enable
        // testOperatorCompilation.testOperatorArguments
        // throw new CompilationException(CAUSE.SCHEMA_INCOMPATIBLE, getOperatorName() + "-"
        // + getOperatorID().toString());
      }

      _schemaMatcher.compile(getInputPorts(), getOutputPorts());
    } catch (Exception e) {
      System.err.println("Compilation failure encountered in operator '" + getOperatorName() + "'.");
      throw e;
    }

  }

  public void runSafetyAnalysis(boolean strict) throws CompilationException {
//    try {
//      performSafetyAnalysis(strict);
//    } catch (IOException e) {
//      Assertion.impossible("We couldn't load the class file but apparently the class loader could: "
//              + e.getMessage());
//    } catch (LeakDetectionException e) {
//      String ref = _functionObject.getClass().getName() + "/" + _algorithm.getMethod().getName();
//      throw new CompilationException(CAUSE.MEMORY_LEAK_DETECTED, ref + " >> " + getOperatorName(), e);
//    }
  }

//  private void performSafetyAnalysis(boolean strict) throws FileNotFoundException, IOException,
//          LeakDetectionException {
////    System.out.println("Performing safety analysis for operator: " + getOperatorName());
//    // At this moment it is clear what parameter actually needs to be counted towards the
//    // state of the operator and therefore needs to be tainted.
//    int[] stateArgs = new int[_arguments.length];
//    // params start at index 1. 'this' is 0.
//    int firstStateArg = _algorithm.getMethod().getParameterTypes().length - _arguments.length + 1;
//    for (int i = 0; i < _arguments.length; i++)
//      stateArgs[i] = firstStateArg + i;
//    OperatorCompiler.compile(_algorithm.getMethod(), stateArgs, strict);
//  }

  // TODO this backward compatibility should go away
  private StatefulFunction detectAlgorithm() throws CompilationException {
    Assertion.invariant(_functionObject != null);
    return StatefulFunction.resolve(_functionObject);
  }

  private boolean isTypeCompatible(Method m, Object[] types, boolean strict) {
    Class<?>[] paramTypes = m.getParameterTypes();
    if (paramTypes.length == types.length) {
      for (int i = 0; i < paramTypes.length; i++) {
        try {
          if (!paramTypes[i].isAssignableFrom((Class<?>) types[i])) {
            if (strict) return false;
            else if (types[i] != Object.class) return false;
          }
        } catch (ClassCastException e) {
          // FIXME this is the case when the type of the object is unknown at compile-time. what
          // we do is pass "java.lang.Object" as a String. we forget to turn it into a class at
          // the clojure side!
          if (RuntimeProcessConfiguration.LOGGING_ENABLED) {
            System.out.println("Compilation failure occured in Ohua: ");
            System.out.println(super.getOperatorName() + ": " + Arrays.deepToString(types));
            System.out.println(e.getMessage());
          }
          // throw e;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public final void runProcessRoutine() {
    _frameworkStats.begin();
    Object[] data = _schemaMatcher.matchInputSchema();
    while (data != null) {
      _fnStats.begin();
      Object results = _funcExec.execute(getOperatorName(), _algorithm, data);
      _fnStats.end();

      _outputSideStats.begin();
      boolean backOff = _schemaMatcher.matchOutputSchema(results);
      _outputSideStats.end();
      if (backOff) {
        break; // backing off due to quanta exhausted or arc boundaries but will be returning
      } else {
        // let the input ports make a decision
      }
      data = _schemaMatcher.matchInputSchema();
    }
    _frameworkStats.end();
  }

  @Override
  public final void prepare() {
    // at first prepare the algorithm
    try {
      _algorithm = detectAlgorithm();
    } catch (CompilationException e) {
      Assertion.impossible("Should have been caught during compilation!");
    }

    AbstractSchemaMatcher old = _schemaMatcher;
    _schemaMatcher = getSchemaMatcher(this, this);
    _schemaMatcher.transfer(old);

    try {
      _schemaMatcher.prepare();
    } catch (Exception e) {
      System.err.println("Hit unexpected exception in function: " + super.getOperatorName());
      if (e instanceof ArrayIndexOutOfBoundsException) {
        System.err.println("This problem is related to issue #147. Please check that you pass the right number of arguments to the function.");
      }
      throw e;
    }

    // FIXME the below belongs into compilation!
    // TODO must be adapted to take only the n last parameters of the algorithm function.
    // TODO compute the array to be filled by the schema matcher for the arguments passed to the
    // function at runtime.
    // performArgumentsMatch();
    initStatisticsCollection();
  }

  /**
   * Checks whether the given arguments match the signature of the provided algorithm.
   */
  @SuppressWarnings("unused")
  // TODO put this back in when working on compilation
  private void performArgumentsMatch() {
    // TODO normally we would inherit this information from the compile-time
    try {
      Class<?>[] typeArgs = new Class[_arguments.length];
      for (int i = 0; i < _arguments.length; i++) {
        typeArgs[i] = _arguments[i].second().getClass();
      }
      _algorithm = detectAlgorithm();
      if (!isTypeCompatible(_algorithm.getMethod(), typeArgs, true)) {
        throw new RuntimeException("Types do not match!");
      }
    } catch (CompilationException e) {
      // because this was already checked by compilation
      Assertion.impossible(e);
    }
  }

  @Override
  public final void cleanup() {
    _schemaMatcher.cleanUp();
  }

  public void logStats(Appendable resource) throws IOException {
    resource.append("{ \"func-id\" : \"" + super.getOperatorName() + "\", \"stats\" : { \"framework\" : ");
    _frameworkStats.log(resource);
    resource.append(", \"function\" : ");
    _fnStats.log(resource);
    resource.append(", \"output-side\" : ");
    _outputSideStats.log(resource);
    resource.append(", ");
    _schemaMatcher.logStats(resource);
    resource.append("} }");
  }

  // TODO provide a more sophisticated and less invasive routine to find and handle operator
  // state.
  @Override
  public final Object getState() {
    // TODO for this to be true, we have to find a way to rerun the Clojure program!
    return _schemaMatcher.getState();
  }

  @Override
  public final void setState(Object checkpoint) {
    _schemaMatcher.setState(checkpoint);
  }

  public void setExplicitOutputSchemaMatch(int[] explicitSourceMatching) {
    SchemaSupport.setExplicitOutputSchemaMatch(this, _schemaMatcher, explicitSourceMatching);
  }

  public void setExplicitOutputSchemaMatch(int portIdx, int[] explicitSourceMatching) {
    SchemaSupport.setExplicitOutputSchemaMatch(this, _schemaMatcher, portIdx, explicitSourceMatching);
  }

  public void setExplicitInputSchemaMatch(int[] explicitTargetMatching, int matchType) {
    SchemaSupport.setExplicitInputSchemaMatch(this, _schemaMatcher, explicitTargetMatching, matchType);
  }

  public void setExplicitInputSchemaMatch(int portIdx, int[] explicitTargetMatching, int matchType) {
    SchemaSupport.setExplicitInputSchemaMatch(this, _schemaMatcher, portIdx, explicitTargetMatching, matchType);
  }

  public Class<?> getParameterType(int formalSchemaIndex) {
    return _algorithm.getMethod().getParameterTypes()[formalSchemaIndex];
  }

  public Annotation[] getParameterAnnotation(int formalSchemaIndex) {
    return _algorithm.getMethod().getParameterAnnotations()[formalSchemaIndex];
  }

  public String getParameterName(int formalSchemaIndex) {
    if (_algorithm.getMethod().getParameters()[formalSchemaIndex].isNamePresent())
      return _algorithm.getMethod().getParameters()[formalSchemaIndex].getName();
    else return "slot-" + formalSchemaIndex;
  }

  public int getFlowArgumentCount() {
    // FIXME in case of compound arguments, this can not be answered! it can not even be derived
    // from the registered input references: think implicit schema match!
    /**
     * The right way to retrieve this information is by looking at the registered input
     * references and resolving implicit schema matches. This is not in place yet. See issue
     * #155. This value therefore must be set by the compilation process. For now, we do the
     * following:<br>
     * If there is no implicit match among the register inputs then we return this number,
     * otherwise we return the max of the registered inputs and the algorithm parameter minus
     * the number of arguments (global variables).<br>
     * Note: this makes this simple getter a bit more expensive.
     */
    int[] inputs = _schemaMatcher.getRegisteredInputs();
    if (Arrays.stream(inputs).filter(i -> i == -1).count() == 0) return inputs.length;
    else return Math.max(getFlowFormalsCount(), inputs.length);
  }

  /**
   * If the last argument is a compound argument and the number of flow arguments and global
   * variable arguments is greater than the number of formal arguments then we will count the
   * last (compound) argument towards the flow formal count.
   */
  public int getFlowFormalsCount() {
    int[] inputs = _schemaMatcher.getRegisteredInputs();
    if (_algorithm.getMethod().getParameterCount() == 0 ||
            (inputs.length + _arguments.length > _algorithm.getMethod().getParameterTypes().length
                    && _schemaMatcher.isBasicArgument(_algorithm.getMethod().getParameterCount()
                    - 1))) return _algorithm.getMethod().getParameterCount();
    else return _algorithm.getMethod().getParameterTypes().length - _arguments.length;
  }

  public Class<?>[] getFormalArguments() {
    return _algorithm.getMethod().getParameterTypes();
  }

  public boolean isLastFormalCompound() {
    int[] inputs = _schemaMatcher.getRegisteredInputs();
    return inputs.length + _arguments.length > _algorithm.getMethod().getParameterTypes().length
            && _schemaMatcher.isBasicArgument(_algorithm.getMethod().getParameterCount() - 1);
  }

  public void setFunctionObject(Object func) {
    _functionObject = func;
  }

  public Class<?> getFunctionType() {
    if (_functionObject != null) return _functionObject.getClass();
    else return null;
  }

  @Override
  public String getFunctionName() {
    return _algorithm.getMethod().getName();
  }

  public void setExecution(FunctionExecution functionExecution) {
    _funcExec = functionExecution;
  }

  public void addOutOfContextInput(String portName) {
    _schemaMatcher.addOutOfContextInput(portName);
  }

  public boolean hasConditionalInput() {
    return _schemaMatcher.hasConditionalInput();
  }

  @Override
  public Class<?> getReturnType() {
    return _algorithm.getMethod().getReturnType();
  }

  protected void setSchemaMatcher(AbstractSchemaMatcher matcher) {
    _schemaMatcher = matcher;
  }

  @Override
  public boolean isComputationComplete() {
    return _schemaMatcher.isComputationComplete();
  }

  @Override
  public boolean useJavaVarArgsSemantics() {
    return _algorithm.getMethod().getDeclaredAnnotation(defsfn.class).useJavaVarArgsSemantics();
  }
}
