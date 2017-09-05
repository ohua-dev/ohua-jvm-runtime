/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.daapi.DataAccess;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;

public class OhuaOperator implements OperatorStateAccess
{
  private AbstractOperatorRuntime _core = null;
  
  public OhuaOperator(AbstractOperatorRuntime core)
  {
    _core = core;
  }
  
  public AbstractOperatorAlgorithm getUserOperator()
  {
    return _core.getOp().getOperatorAlgorithm();
  }

  public final void setProcessState(SystemPhaseType t) { _core.setProcessState(t); }

  public final SystemPhaseType getProcessState()
  {
    return _core.getProcessState();
  }
  
  public final List<InputPort> getInputPorts()
  {
    return _core.getOp().getInputPorts();
  }

  public final List<OutputPort> getOutputPorts()
  {
    return _core.getOp().getOutputPorts();
  }
  
  public final boolean broadcast(IMetaDataPacket data)
  {
    return _core.broadcast(data);
  }

  public final boolean broadcast(IMetaDataPacket data, boolean broadCastToMetaPorts)
  {
    return _core.broadcast(data, broadCastToMetaPorts);
  }

  public final void enqueueMetaData(OutputPort port, IMetaDataPacket data){
    _core.enqueueMetaData(port, data);
  }

  public final OperatorID getId()
  {
    return _core.getOp().getId();
  }

  public final Object getState()
  {
    return _core.getOp().getState();
  }
  
  public final void setState(Object checkpoint)
  {
    _core.getOp().setState(checkpoint);
  }
  
  /**
   * The operator has no user input connections.
   * @return
   */
  public boolean isSourceOperator()
  {
    return _core.getOp().isSystemOutputOperator();
  }
  
  /**
   * The operator has no user output connections.
   * @return
   */
  public boolean isTargetOperator()
  {
    return _core.getOp().isSystemInputOperator();
  }
  
  public DataAccess getDataAccess()
  {
    return _core.getOp().getDataLayer();
  }
}
