/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

import ohua.runtime.engine.Maybe;
import ohua.runtime.engine.points.IDataPollOnBlockedInputPortHandler;
import ohua.runtime.engine.points.InputPortEvents;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.packets.DataPollOnBlockedInputPortEvent;
import ohua.util.Tuple;

public class DataPollOnBlockedInputPortSignalMixin extends
                                                  VisitorMixin<DataPollOnBlockedInputPortEvent, IDataPollOnBlockedInputPortHandler>
{
  
  public DataPollOnBlockedInputPortSignalMixin(InputPort in)
  {
    super(in);
  }
  
  @Override
  public Tuple<InputPort.VisitorReturnType, Maybe<Object>> handlePacket(DataPollOnBlockedInputPortEvent packet)
  {
    notifyHandlers(packet);
    return null; // FIXME!
  }
  
  public void notifyHandlers(DataPollOnBlockedInputPortEvent packet)
  {
    for(IDataPollOnBlockedInputPortHandler handler : getAllHandlers())
    {
      handler.notifyMarkerArrived(_inputPort, packet);
    }
  }
  
  public InputPortEvents getEventResponsibility()
  {
    return InputPortEvents.DATA_POLL_ON_BLOCKED_INPUT_PORT;
  }
  
}
