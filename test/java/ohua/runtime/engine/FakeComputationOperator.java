/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.Serializable;
import java.util.Random;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class FakeComputationOperator extends UserOperator
{
  public static enum Time
  {
    MS
    {
      public long time()
      {
        return System.currentTimeMillis();
      }
    },
    
    NS
    {
      public long time()
      {
        return System.nanoTime();
      }
    };
    
    abstract public long time();
  }
  
  public static class FakeComputationOperatorProperties implements Serializable
  {
    public long computationTime = 10;
    public int packetProjection = 1;
    public boolean useRandom = false;
    public String keyField = "";
    public Time time = Time.NS;
  }
  
  public FakeComputationOperatorProperties _properties =
      new FakeComputationOperatorProperties();
  
  private InputPortControl _inPortControl = null;
  private OutputPortControl _outPortControl = null;
  
  // state: outstanding write
  private int _outstandingWrites = 0;
  
  @Override
  public void cleanup()
  {
    // nothing
  }
  
  @Override
  public void prepare()
  {
    _inPortControl = getDataLayer().getInputPortController("input");
    _outPortControl = getDataLayer().getOutputPortController("output");
  }
  
  @Override
  public void runProcessRoutine()
  {
    // System.out.println("Computation time: " + _properties.computationTime);
    if(!finishWrite())
    {
      return;
    }
    
    while(_inPortControl.next())
    {
      doDummyComputation();
      
      if(_properties.useRandom)
      {
        int nextInt = new Random(System.currentTimeMillis()).nextInt();
        nextInt = Math.abs(nextInt);
        _outstandingWrites = nextInt % _properties.packetProjection;
      }
      else
      {
        _outstandingWrites = _properties.packetProjection;
      }
      
      if(!finishWrite())
      {
        return;
      }
    }
  }
  
  private boolean finishWrite()
  {
    while(_outstandingWrites > 0)
    {
      getDataLayer().transferInputToOutput(_inPortControl.getPortName(),
                                           _outPortControl.getPortName());
      if(_properties.keyField.length() > 0)
      {
        String value =
            _outPortControl.getData(_properties.keyField).get(0).toString() + "-"
                + _outstandingWrites;
        _outPortControl.setData(_properties.keyField, value);
      }
      
      _outstandingWrites--;
      if(_outPortControl.send())
      {
        return false;
      }
    }
    return true;
  }
  
  private void doDummyComputation()
  {
    long start = _properties.time.time();
    long end = _properties.time.time();
    @SuppressWarnings("unused")
    int addSomething = 0;
    while(end - start < _properties.computationTime)
    {
      // waste cpu time!
      for(int i = 0; i < 5; i++)
      {
        addSomething++;
      }
      end = _properties.time.time();
    }
    // System.out.println("Computations: " + addSomething);
  }
  
  public Object getState()
  {
    return null;
  }
  
  public void setState(Object state)
  {
    prepare();
  }
  
}
