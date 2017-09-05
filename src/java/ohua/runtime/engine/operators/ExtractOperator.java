/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.operators;

import java.io.Serializable;
import java.util.ArrayList;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class ExtractOperator extends UserOperator
{
  public static class ExtractOperatorProperties implements Serializable
  {
    public ArrayList<String> toBeExtracted = null;
  }
  
  public ExtractOperatorProperties properties = null;
  
  private InputPortControl _inControl = null;
  private OutputPortControl _extractOutControl = null;
  private OutputPortControl _outControl = null;
      
  @Override
  public void prepare()
  {
    _inControl = getDataLayer().getInputPortController("input");
    _outControl = getDataLayer().getOutputPortController("output");
    _extractOutControl = getDataLayer().getOutputPortController("extracted");
  }
  
  @Override
  public void runProcessRoutine()
  {
    boolean backoff = false;
    while(!backoff && _inControl.next())
    {
      _extractOutControl.newPacket();
      for(String extract : properties.toBeExtracted)
      {
        getDataLayer().transfer("input", "extracted", extract);
      }
      backoff = _extractOutControl.send();
      getDataLayer().transferInputToOutput("input", "output");
      backoff |= _outControl.send();
    }
  }
  
  @Override
  public void cleanup()
  {
    // nothing 
  }
  
  @Override
  public Object getState()
  {
    return null;
  }
  
  @Override
  public void setState(Object state)
  {
    // stateless
  }
}
