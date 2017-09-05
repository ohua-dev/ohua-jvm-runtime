/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import java.util.Set;

import ohua.runtime.engine.flowgraph.elements.packets.IPacket;

public interface IPacketVisitor<T extends IPacketHandler, S extends IPacket>
{
  public void registerMarkerHandler(T handler);
  
  public void notifyHandlers(S packet);
  
  public void unregisterMarkerHandler(T handler);
  
  public InputPortEvents getEventResponsibility();
  
  public Set<T> getAllHandlers();
}
