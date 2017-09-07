/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators.system;

import java.util.Collections;
import java.util.LinkedList;

import ohua.runtime.engine.flowgraph.elements.operator.OutputPort;
import ohua.runtime.engine.flowgraph.elements.operator.SystemOperator;
import ohua.runtime.engine.flowgraph.elements.packets.ActivationMarker;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfStreamPacket;
import ohua.runtime.engine.flowgraph.elements.packets.IMetaDataPacket;
import ohua.runtime.engine.operators.OperatorDescription;

public class ProcessControlOperator extends SystemOperator
{

  public static OperatorDescription description(){
    OperatorDescription desc = new OperatorDescription();
    desc.setInputPorts(Collections.EMPTY_LIST);
    desc.setOutputPorts(Collections.singletonList("output"));
    return desc;
  }

  private ExternalMetaInput _externalInput = null;
  
  public interface ExternalMetaInput
  {
    void push(LinkedList<IMetaDataPacket> packets);
    boolean isInputAvailable();
    IMetaDataPacket poll();
  }
   
  private IMetaDataPacket _lastPacket = null;
  
  public void setExternalInput(ExternalMetaInput externalInput)
  {
    _externalInput = externalInput;
  }
  
  @Override
  public void cleanup()
  {
    // nothing
  }
  
  @Override
  public void prepare()
  {
    runProcessRoutine();
  }
  
  @Override
  public void runProcessRoutine()
  {
    if(!_externalInput.isInputAvailable())
    {
      return;
    }
    
    IMetaDataPacket packet = _externalInput.poll();
    while(packet != null)
    {
      if(packet instanceof ActivationMarker)
      {
        for(OutputPort outPort : getOutputPortsImpl())
        {
          outPort.activate();
        }
      }
      
      broadcast(packet);
      
      _lastPacket = packet;
      packet = _externalInput.poll();
    }
  }
    
  public void inject(LinkedList<IMetaDataPacket> packets)
  { 
    _externalInput.push(packets);
    
    // we want runProcessRoutine() to be called for this op. even if it is a system phase.
    for(OutputPort outPort : getOutputPortsImpl())
    {
      outPort.open();
    }
  }

  /**
   * This routine is very important because it decides when to shutdown the whole flow. For an
   * ETL flow we want to say that the shutdown can happen as regulated by the source operators.
   * That means if they do not have any more data from their source then the computation is
   * supposed to finish. However in the case of online streaming or EAI (operators that read
   * from messaging queues) the operators can not know when to stop retrieving data/events from
   * their source as there is an infinite amount of data arriving. For this case this
   * ProcessControl operator must keep the flow alive for as long as the user who started the
   * flow wants to. Hence the user can submit a shutdown request through the ProcessManager
   * interface which ultimately results in a shutdown packet interpreted by this operator which
   * forces it to return true in this function.
   */
  @Override
  protected boolean isSourceOperatorWasLastPacket()
  {
    return _lastPacket != null && _lastPacket instanceof EndOfStreamPacket
           && ((EndOfStreamPacket) _lastPacket).portsToClose().isEmpty();
  }
}
