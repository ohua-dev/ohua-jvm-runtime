/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

import ohua.runtime.engine.daapi.DataAccess;

public abstract class OperatorAlgorithmAdapter implements OperatorStateAccess
{
  protected OperatorCore _operatorCore = null;
  protected AbstractOperatorAlgorithm _operatorAlgorithm = null;

  protected OperatorAlgorithmAdapter(OperatorCore operatorCore,
                                     AbstractOperatorAlgorithm operatorAlgorithm)
  {
    _operatorCore = operatorCore;
    _operatorAlgorithm = operatorAlgorithm;
  }
  
  abstract protected boolean isOutputFavored();
  
  abstract protected boolean isInputFavored();
  
  abstract protected boolean isSourceOperatorWasLastPacket();
  
  abstract protected void prepareInputPorts();

  public DataAccess getDataLayer()
  {
    return _operatorCore.getDataLayer();
  }
  
  public String getOperatorName()
  {
    return _operatorCore.getOperatorName();
  }
  
  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public List<String> getInputPorts()
  {
    return _operatorCore.getInputPortNames();
  }
  
  /**
   * The order in which these names are returned maps to the port registration order.
   * @return
   */
  public List<String> getOutputPorts()
  {
    return _operatorCore.getOutputPortNames();
  }
  
  abstract public boolean isSystemComponent();
  
  protected Class<?> getImplementationClass()
  {
    return _operatorAlgorithm.getClass();
  }

  public OperatorID getOperatorID()
  {
    return _operatorCore.getId();
  }

}
