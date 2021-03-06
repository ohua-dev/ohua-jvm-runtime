/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;
import ohua.runtime.engine.flowgraph.elements.packets.EndOfOperatorInitMarker;

public interface IEndOfOperatorInitMarkerHandler extends IPacketHandler
{
  public void notifyMarkerArrived(InputPort port, EndOfOperatorInitMarker packet);
}
