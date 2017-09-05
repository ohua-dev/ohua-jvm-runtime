/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import java.util.List;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.flowgraph.elements.operator.PortID;


public interface EndOfStreamPacket extends IMetaDataPacket
{
  public int getLevelToClose();
  
  public SystemPhaseType getType();
  
  public List<PortID> portsToClose();
}
