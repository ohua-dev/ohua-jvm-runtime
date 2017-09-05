/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import ohua.runtime.engine.points.InputPortEvents;


public interface IPacket
{
  /**
   * General packet interface needed for generics of visitor mixin.<br>
   * This is the interface for all information in the system!
   */ 
  public InputPortEvents getEventType();
}
