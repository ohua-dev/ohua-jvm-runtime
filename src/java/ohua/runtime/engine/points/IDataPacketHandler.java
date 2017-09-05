/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import ohua.runtime.engine.daapi.DataPacket;
import ohua.runtime.engine.flowgraph.elements.operator.InputPort;

public interface IDataPacketHandler extends IPacketHandler
{
  public void notifyMarkerArrived(InputPort port, DataPacket packet);
}
