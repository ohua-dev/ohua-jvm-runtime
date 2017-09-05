/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

public interface DataPollOnBlockedInputPortEvent extends ISystemEventPacket
{
  /**
   * This is a notification that the operator logic of the current operator tries to retrieve
   * data from an input port that is in BLOCKED state.
   */
}
