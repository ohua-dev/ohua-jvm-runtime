/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

/**
 * The class responsible for handling the calls to the operator.
 * @author sertel
 * 
 */
public class OperatorExecution
{
  public volatile boolean _discardExceptions = false;
  public Exception _caught = null;
  
  protected void prepare(AbstractOperatorAlgorithm algo)
  {
    try{
      algo.prepare();
    }catch(Exception e){
      System.err.println("Caught exception at operator: " + algo.getOperatorName());
      throw e;
    }
  }
  
  protected void run(AbstractOperatorAlgorithm algo)
  {
    try
    {
      algo.runProcessRoutine();
    }
    catch(Exception e)
    {
      if(_discardExceptions)
      {
        // we just return normally and store the exception
        _caught = e;
      }
      else
      {
        System.out.println("Caught exception at operator: " + algo.getOperatorName());
        // the normal case: just rethrow
        throw e;
      }
    }
  }
  
  protected void cleanup(AbstractOperatorAlgorithm algo)
  {
    algo.cleanup();
  }
}
