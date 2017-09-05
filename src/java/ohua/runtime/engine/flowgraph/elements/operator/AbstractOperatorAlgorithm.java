/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

import ohua.runtime.engine.daapi.DataAccess;

public abstract class AbstractOperatorAlgorithm
{
  private OperatorAlgorithmAdapter _adapter = null;
  
  protected void setOperatorAlgorithmAdapter(OperatorAlgorithmAdapter core)
  {
    _adapter = core;
  }
  
  // this step should simulate the state build during compile-time as well as the inits needed
  // in real operators for the runtime process routine
  // this marks the very first state of an operator
  public abstract void prepare();
  
  // this will be a dummy implementation that simulates some computation that behaves similar
  // in terms of using CPU and RAM to a real operator
  public abstract void runProcessRoutine();
  
  // this will the hook for the operators to clean up their state (closing connections etc.)
  // after they have finished processing
  public abstract void cleanup();
  
  public final DataAccess getDataLayer()
  {
    return _adapter.getDataLayer();
  }

  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public final List<String> getInputPorts()
  {
    return _adapter.getInputPorts();
  }
  
  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public final List<String> getOutputPorts()
  {
    return _adapter.getOutputPorts();
  }

  public final String getOperatorName()
  {
    return _adapter.getOperatorName();
  }
  
  public final OperatorID getOperatorID()
  {
    return _adapter.getOperatorID();
  }

}
