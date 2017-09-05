/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.ActivationMarkerVisitorMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.DataPollOnBlockedInputPortSignalMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.DataSignalVisitorMixin;
import ohua.runtime.engine.flowgraph.elements.packets.functionality.EndStreamSignalVisitorMixin;

public class VisitorFactory
{
  public static EndStreamSignalVisitorMixin createEndStreamPacketVisitor(InputPort port)
  {
    EndStreamSignalVisitorMixin mixin = new EndStreamSignalVisitorMixin(port);
    return mixin;
  }
  
  public static DataSignalVisitorMixin createDataPacketVisitor(InputPort port)
  {
    DataSignalVisitorMixin mixin = new DataSignalVisitorMixin(port);
    return mixin;
  }
  
  public static DataPollOnBlockedInputPortSignalMixin createDataPollOnBlockedPortEventVisitor(InputPort port)
  {
    DataPollOnBlockedInputPortSignalMixin mixin = new DataPollOnBlockedInputPortSignalMixin(port);
    return mixin;
  }

  public static ActivationMarkerVisitorMixin createActivationMarkerVisitor(InputPort inPort)
  {
    ActivationMarkerVisitorMixin mixin = new ActivationMarkerVisitorMixin(inPort);
    return mixin;
  }

}
