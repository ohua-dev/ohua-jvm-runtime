/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

public interface ISystemEventPacket extends IPacket
{
  /**
   * Inter-Layer Events:<br>
   * This interface marks information given to any handler that registers for it and can provide
   * system event information up to certain handlers.
   * <p>
   * The system will NOT support propagating those events through its arcs!
   */
}
