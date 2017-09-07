/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.operators;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

public class PeekOperator extends UserOperator
{
  public static OperatorDescription description(){
    OperatorDescription desc = new OperatorDescription();
    desc.setInputPorts(Collections.singletonList("input"));
    desc.setOutputPorts(Collections.singletonList("output"));
    return desc;
  }

  private InputPortControl _inPortControl = null;
  private OutputPortControl _outPortControl = null;
  
  private long _totalSeen = 0;

  @Override
  public void prepare()
  {
    _inPortControl = getDataLayer().getInputPortController("input");
    _outPortControl = getDataLayer().getOutputPortController("output");
  }
  
  @Override
  public void runProcessRoutine()
  {
    int amountSent = 0;
    while(_inPortControl.next())
    {
//      String msg = "received data packet: " + _inPortControl.dataToString("XML");
//      OhuaLoggerFactory.getLogger(getLogger().getName() + "-packet-log").info(msg);
//      getLogger().fine(msg);
      
      _totalSeen++;
      amountSent++;
      getDataLayer().transferInputToOutput("input", "output");
      if(_outPortControl.send())
      {
        // getLogger().log(Level.ALL,
        // "is boundary respected for output port = " + isRespectBoundaries(0));
//        getLogger().info("send " + amountSent + " data packets downstream");

        return;
      }
    }
    
//    getLogger().info("send " + amountSent + " data packets downstream");
//    getLogger().info("send (total) " + _totalSeen + " data packets downstream");
  }
  
  public Object getState()
  {
    Logger.getLogger("debug_cpInitiator").log(Level.ALL, "CHECKPOINT OF Peek taken");
    return null;
  }

  @Override
  public void cleanup()
  {
    // nothing to do here
  }
  
  public void setState(Object checkpoint)
  {
    // since this operator is completely stateless, we do not have to recover any state
    prepare();
  }

  
}
