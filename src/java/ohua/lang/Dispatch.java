/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.util.Tuple;
import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.lang.operator.IFunctionalOperator;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;
import ohua.runtime.exceptions.CompilationException;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.stream.Collectors;

public class Dispatch extends UserOperator implements IFunctionalOperator {

    /*
     * IFunctionalOperator code starts here:
     */

  public boolean isAlgoVarArgs(){
    return false;
  }

  @Override
  public int getFlowArgumentCount() {
    return 0;
  }

  @Override
  public int getFlowFormalsCount() {
    return 0;
  }

  @Override
  public Class<?>[] getFormalArguments() {
    return new Class<?>[0];
  }

  @Override
  public Tuple<Integer, Object>[] getEnvironmentArguments() {
    return new Tuple[0];
  }

  @Override
  public Class<?> getParameterType(int formalSchemaIndex) {
    Assertion.invariant(false, "Should never be called for this operator.");
    return null;
  }

  @Override
  public Class<?> getReturnType() {
    return Object.class;
  }

  @Override
  public Annotation[] getParameterAnnotation(int formalSchemaIndex) {
    Assertion.invariant(false, "Should never be called for this operator.");
    return null;
  }

  @Override
  public String getParameterName(int formalSchemaIndex) {
    Assertion.invariant(false, "Should never be called for this operator.");
    return null;
  }

  @Override
  public String getFunctionName() {
    return "dispatch";
  }

  @Override
  public void compile(boolean typeSensitive) throws CompilationException {
    // nothing to compile here
  }

  @Override
  public void runSafetyAnalysis(boolean strict) throws CompilationException {
    // nothing really
  }

  @Override
  public void setExplicitInputSchemaMatch(int[] explicitTargetMatching, int matchType) {
    // nothing to be done
  }

  @Override
  public void setExplicitInputSchemaMatch(int portIdx, int[] explicitTargetMatching, int matchType) {
    // nothing to be done
  }

  @Override
  public void setExplicitOutputSchemaMatch(int[] explicitSourceMatching) {
    // nothing to be done
  }

  @Override
  public void setExplicitOutputSchemaMatch(int portIdx, int[] explicitSourceMatching) {
    // nothing to be done
  }

  @Override
  public void setArguments(Tuple<Integer, Object>[] arguments) {
    Assertion.invariant(false, "Should never be called for this operator.");
  }

    /*
     * UserOperator functionality starts here:
     */

  private InputPortControl _inControl = null;
  private List<OutputPortControl> _outControls = null;

  // state
  private int _currentOutput = 0;

  @Override
  public void prepare() {
    List<String> inPorts = super.getInputPorts();
    assert inPorts.size() == 1;
    _inControl = super.getDataLayer().getInputPortController(inPorts.get(0));
    _outControls = super.getOutputPorts().stream().map(
            l -> super.getDataLayer().getOutputPortController(l)
    ).collect(Collectors.toList());
  }

  // TODO @Operator annotation missing that gets picked up by the Linker
  @Override
  public void runProcessRoutine() {
    while(!_inControl.hasSeenLastPacket() && _inControl.next()){
      super.getDataLayer().transferInputToOutput(_inControl.getPortName(), _outControls.get(_currentOutput).getPortName());
      boolean returnControl = _outControls.get(_currentOutput).send();
      _currentOutput = (_currentOutput + 1) % _outControls.size();
      if(returnControl) break;
    }
  }

  @Override
  public void cleanup() {
    // nothing to be cleaned up
  }

  @Override
  public Object getState() {
    return _currentOutput;
  }

  @Override
  public void setState(Object state) {
    _currentOutput = (int) state;
  }
}