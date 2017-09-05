/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.lang.OneToNSupport;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.utils.GraphVisualizer;
import ohua.runtime.exceptions.CompilationException;


public abstract class OhuaProgram extends OhuaFrontend
{

  // TODO do the same for the output schema
  protected void registerInputSchema(int target, int[] explicitTargetMatching, int matchType) {
    super.registerInputSchema(target, explicitTargetMatching, matchType);
  }
  
  public void registerDependency(int source, int sourcePos, int target, int targetPos, int isFeedback) {
//    System.out.println("Register dependency: " + source + " " + target + " " + targetPos + " " + isFeedback);
    super.registerDependency(source, sourcePos, target, targetPos, isFeedback);
  }
  
  protected final void prepare() {
    super.resolveDependencies();

    // TODO we need an interface for transformation
    // FIXME these algorithms most likely want to be implemented on top of the new dataflow IR
    OneToNSupport.prepare(_process.getGraph());
    GraphVisualizer.printFlowGraph(_process.getGraph());
  }
  
  public void createOperator(String type, int id) throws OperatorLoadingException, CompilationException {
      super.createOperator(type, id);
  }

}
