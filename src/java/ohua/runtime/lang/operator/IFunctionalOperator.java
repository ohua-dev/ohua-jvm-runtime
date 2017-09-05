/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import java.lang.annotation.Annotation;

import ohua.util.Tuple;
import ohua.runtime.exceptions.CompilationException;

public interface IFunctionalOperator {
  // meta info
  int getFlowArgumentCount();
  
  int getFlowFormalsCount();
  
  Class<?>[] getFormalArguments();
  
  Tuple<Integer, Object>[] getEnvironmentArguments();
  
  Class<?> getParameterType(int formalSchemaIndex);
  
  Class<?> getReturnType();
  
  Annotation[] getParameterAnnotation(int formalSchemaIndex);
  
  String getParameterName(int formalSchemaIndex);
  
  String getFunctionName();
  
  void compile(boolean typeSensitive) throws CompilationException;

  void runSafetyAnalysis(boolean strict) throws CompilationException;
  
  void setExplicitInputSchemaMatch(int[] explicitTargetMatching, int matchType);
  
  void setExplicitInputSchemaMatch(int portIdx, int[] explicitTargetMatching, int matchType);
  
  void setExplicitOutputSchemaMatch(int[] explicitSourceMatching);
  
  void setExplicitOutputSchemaMatch(int portIdx, int[] explicitSourceMatching);
  
  void setArguments(Tuple<Integer, Object>[] arguments);
  
  default Class<?> getFunctionType() {
    return Object.class;
  }

  boolean isAlgoVarArgs();

  default boolean useJavaVarArgsSemantics() { return true; }
}
