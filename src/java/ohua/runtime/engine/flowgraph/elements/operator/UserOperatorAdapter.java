/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;


public class UserOperatorAdapter extends OperatorAlgorithmAdapter
{
  public UserOperatorAdapter(OperatorCore operatorCore,
                                AbstractOperatorAlgorithm operatorAlgorithm)
  {
    super(operatorCore, operatorAlgorithm);
  }

  @Override
  protected boolean isInputFavored()
  {
    return true;
  }
  
  @Override
  protected boolean isOutputFavored()
  {
    return true;
  }

  @Override
  protected boolean isSourceOperatorWasLastPacket()
  {
    return true;
  }

  @Override
  protected void prepareInputPorts()
  {
    // nothing
  }
  
  public Object getState()
  {
    return ((UserOperator) _operatorAlgorithm).getState();
  }
  
  public void setState(Object state)
  {
    ((UserOperator) _operatorAlgorithm).setState(state);
  }
  
  @Override
  public boolean isSystemComponent()
  {
    return false;
  }
  
}
