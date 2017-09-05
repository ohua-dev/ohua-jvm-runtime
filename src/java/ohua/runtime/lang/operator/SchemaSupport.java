/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

import java.util.List;

// TODO once we switch to Java 8, this functionality main want to be migrated into the IFunctionalOperator interface as "default" implementations.
public class SchemaSupport {
  public static void setExplicitOutputSchemaMatch(UserOperator op, AbstractSchemaMatcher matcher,
                                                  int[] explicitSourceMatching)
  {
    // Assumption: it refers to the last output port that was registered
    List<String> outputPorts = op.getOutputPorts();
    setExplicitOutputSchemaMatch(op, matcher, outputPorts.size() - 1, explicitSourceMatching);
  }
  
  public static void setExplicitOutputSchemaMatch(UserOperator op, AbstractSchemaMatcher matcher, int portIdx,
                                                  int[] explicitSourceMatching)
  {
    if(explicitSourceMatching == null) throw new RuntimeException("i'm still alive!");
    List<String> outputPorts = op.getOutputPorts();
    matcher.registerOutputExplicitSchemaMatch(outputPorts.get(portIdx), explicitSourceMatching);
  }
  
  public static void setExplicitInputSchemaMatch(UserOperator op, AbstractSchemaMatcher matcher,
                                                 int[] explicitTargetMatching, int matchType)
  {
    // Assumption: it refers to the last input port that was registered
    List<String> inputPorts = op.getInputPorts();
    setExplicitInputSchemaMatch(op, matcher, inputPorts.size() - 1, explicitTargetMatching, matchType);
  }
  
  public static void setExplicitInputSchemaMatch(UserOperator op, AbstractSchemaMatcher matcher, int portIdx,
                                                 int[] explicitTargetMatching, int matchType)
  {
    // FIXME dead code
    if(explicitTargetMatching == null) throw new RuntimeException("i'm still alive!");
    List<String> inputPorts = op.getInputPorts();
    matcher.registerInputExplicitSchemaMatch(inputPorts.get(portIdx), explicitTargetMatching, matchType);
  }
}
