/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;

public class SystemOperatorAdapter extends OperatorAlgorithmAdapter {

  private AbstractOperatorRuntime _runtime;

  protected SystemOperatorAdapter(OperatorCore operator, AbstractOperatorAlgorithm operatorAlgorithm) {
    super(operator, operatorAlgorithm);
  }

  @Override
  protected boolean isInputFavored() {
    return false;
  }
  
  @Override
  protected boolean isOutputFavored() {
    return true;
  }
  
  public List<InputPort> getMetaInputPorts() {
    return _operatorCore.getMetaInputPorts();
  }
  
  public List<OutputPort> getOutputPortsImpl() {
    return _operatorCore.getOutputPorts();
  }
  
  public SystemPhaseType getProcessState() {
    return _runtime.getProcessState();
  }
  
  @Override
  protected boolean isSourceOperatorWasLastPacket() {
    return ((SystemOperator) _operatorAlgorithm).isSourceOperatorWasLastPacket();
  }

  @Override
  protected void prepareInputPorts() {
    ((SystemOperator) _operatorAlgorithm).prepareInputPorts();
  }
  
  /**
   * Not sure yet if system operators carry any state or want to participate in checkpointing.
   */
  public Object getState() {
    return _operatorAlgorithm instanceof OperatorStateAccess ? ((OperatorStateAccess) _operatorAlgorithm).getState() : null;
  }
  
  public void setState(Object state) {
    if(_operatorAlgorithm instanceof OperatorStateAccess) ((OperatorStateAccess) _operatorAlgorithm).setState(state);
  }
  
  @Override
  public boolean isSystemComponent() {
    return true;
  }

  protected final boolean broadcast(IMetaDataPacket data){
    return _runtime.broadcast(data);
  }

  public static void setRuntime(AbstractOperatorRuntime runtime){
    if(runtime.getOp().isSystemComponent())
      ((SystemOperatorAdapter) runtime.getOp().getOperatorAdapter())._runtime = runtime;
  }
}
