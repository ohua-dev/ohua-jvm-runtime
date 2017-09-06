/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.Operator;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;
import ohua.runtime.engine.utils.parser.OperatorDescription;

public class GeneratorOperator extends UserOperator
{
  public static OperatorDescription description(){
    OperatorDescription desc = new OperatorDescription();
    desc.setInputPorts(Collections.EMPTY_LIST);
    desc.setOutputPorts(Collections.singletonList("output"));
    return desc;
  }

  public static class GeneratorProperties implements Serializable
  {
    private int _amountToGenerate;
    private int _startOffset = 0;
    private List<String> _schema;

    public int getAmountToGenerate()
    {
      return _amountToGenerate;
    }

    public void setAmountToGenerate(int amountToGenerate)
    {
      _amountToGenerate = amountToGenerate;
    }

    public int getStartOffset()
    {
      return _startOffset;
    }

    public void setStartOffset(int startOffset)
    {
      _startOffset = startOffset;
    }

    public void setSchema(List<String> schema){
      _schema = schema;
    }

  }
  
  private int _alreadySent = 0;
  
  private GeneratorProperties _properties = null;
  
  private OutputPortControl _outPortControl = null;

  @Override
  public void prepare()
  {
    _outPortControl = getDataLayer().getOutputPortController("output");
  }
  
  @Override
  public void runProcessRoutine()
  {
    int sendInThisStep = 0;
    for(int i = _alreadySent + 1; i < _properties.getAmountToGenerate() + 1; i++)
    {
      _outPortControl.newPacket();
      for(String leafPath : _properties._schema)
      {
        int value = _properties.getStartOffset() + i;
        _outPortControl.setData(leafPath, getTestValue(leafPath, value));
      }

      sendInThisStep++;
      // send one data packet
      if(_outPortControl.send())
      {
        break;
      }
    }
    
    _alreadySent += sendInThisStep;
  }

  private String getTestValue(String leafPath, int value)
  {
    List<Object> val = _outPortControl.getData(leafPath);
    
//    if(val != null && val.size() == 1 && val.get(0).toString().length() > 0)
//    {
//      Object defaultValue = val.get(0);
//      if(defaultValue.equals("int"))
//      {
//        return Integer.toString(value);
//      }
//      else if(defaultValue.toString().startsWith("mod"))
//      {
//        String modNumber = defaultValue.toString().substring(4);
//        int mod = Integer.parseInt(modNumber.trim());
//        return Integer.toString(value % mod);
//      }
//      else
//      {
//        return defaultValue.toString() + "-" + value;
//      }
//    }
    
    // default
    return "testValue-" + value;
  }
  
  @Override
  public void cleanup()
  {
    // nothing to do here
  }

  public Object getState()
  {
    return new Object[] {_alreadySent, _properties.getAmountToGenerate(), _properties.getStartOffset()};
  }

  public void setState(Object state)
  {
    Object[] s = (Object[]) state;
    
    _alreadySent = (int) s[0];
    _properties.setAmountToGenerate((int) s[1]);
    _properties.setStartOffset((int) s[2]);

    prepare();
  }
  
  public int getAlreadySent()
  {
    return _alreadySent;
  }
  
  public GeneratorProperties getProperties()
  {
    return _properties;
  }

  public void setProperties(GeneratorProperties properties)
  {
    _properties = properties;
  }
  
  public boolean isUnboundedInput()
  {
    return false;
  }
  
}
