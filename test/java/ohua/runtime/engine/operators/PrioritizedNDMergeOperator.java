/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.operators;

import java.io.Serializable;

import ohua.runtime.engine.daapi.InputPortControl;

public class PrioritizedNDMergeOperator extends NonDeterministicMergeOperator
{
  public static class PrioritizedNDMergeProperties implements Serializable
  {
    public String prioritizedPort = null;
    public int fairness = 50;
  }
  
  public PrioritizedNDMergeProperties properties = null;
  
  private InputPortControl _prioritizedInput = null;
  
  @Override
  public void prepare()
  {
    super.prepare();
    _prioritizedInput = getDataLayer().getInputPortController(properties.prioritizedPort);

    // enforce priority already here
    prioritize();
  }

  private void prioritize()
  {
    _openInputPorts.remove(_prioritizedInput);
    _openInputPorts.addFirst(_prioritizedInput);
  }
  
  protected boolean isSwitchPorts()
  {
    if(_openInputPorts.getFirst() != _prioritizedInput)
    {
      // be more aggressive and put priority port back into list
      _openInputPorts.remove(_prioritizedInput);
      _openInputPorts.add(1, _prioritizedInput);
      
      // always check the priority port
      return true;
    }
    else
    {
      return _packetsDequeuedFromCurrentPort > properties.fairness;
    }
  }
  
  @Override
  public void runProcessRoutine()
  {
    prioritize();
    super.runProcessRoutine();
  }
}
