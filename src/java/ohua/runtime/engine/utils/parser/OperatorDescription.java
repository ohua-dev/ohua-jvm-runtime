/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils.parser;

import java.util.List;

import org.exolab.castor.mapping.Mapping;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;

public class OperatorDescription implements Cloneable
{
  private List<String> _inputPorts = null;
  private List<String> _outputPorts = null;
  
  private boolean _dynamicInputPorts = false;
  private boolean _dynamicOutputPorts = false;

//  private Mapping _propertiesMapping = null;
  private Class<?> _properties = null;
  
  public void apply(OperatorCore operator, boolean isUserOperator)
  {
    for(String inputPort : _inputPorts)
    {
      InputPort inPort = addNewInputPort(operator, inputPort);
      inPort.setMetaPort(!isUserOperator);
    }
    
    for(String outputPort : _outputPorts)
    {
      OutputPort outPort = addNewOutputPort(operator, outputPort);
      outPort.setMetaPort(!isUserOperator);
    }
  }

  public List<String> getInputPorts()
  {
    return _inputPorts;
  }

  public void setInputPorts(List<String> inputPorts)
  {
    _inputPorts = inputPorts;
  }

  public List<String> getOutputPorts()
  {
    return _outputPorts;
  }

  public void setOutputPorts(List<String> outputPorts)
  {
    _outputPorts = outputPorts;
  }

//  public Mapping getPropertiesMapping()
//  {
//    return _propertiesMapping;
//  }
//
//  public void setPropertiesMapping(Mapping propertiesMapping)
//  {
//    _propertiesMapping = propertiesMapping;
//  }
    
  public void setDyanmicInputPorts(boolean dynamic)
  {
    _dynamicInputPorts = dynamic;
  }
  
  public boolean hasDynamicInputPorts()
  {
    return _dynamicInputPorts;
  }
  
  public void setDynamicOutputPorts(boolean dynamic)
  {
    _dynamicOutputPorts = dynamic;
  }
  
  public boolean hasDynamicOutputPorts()
  {
    return _dynamicOutputPorts;
  }
  
  public static InputPort addNewInputPort(OperatorCore operator, String inputPort)
  {
    InputPort inPort = new InputPort(operator);
    inPort.setPortName(inputPort);
    operator.addInputPort(inPort);
    return inPort;
  }
  
  public static OutputPort addNewOutputPort(OperatorCore operator, String outputPort)
  {
    OutputPort outPort = new OutputPort(operator);
    outPort.setPortName(outputPort);
    operator.addOutputPort(outPort);
    return outPort;
  }
}
