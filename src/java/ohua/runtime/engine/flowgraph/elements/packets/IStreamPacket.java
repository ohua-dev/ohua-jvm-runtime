/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets;

import java.io.Serializable;

/**
 * Streaming Layer Events:<br>
 * Information marked with this interface will have the possibility to be propagated through the
 * arcs to other operators in the system.
 */
public interface IStreamPacket extends IPacket, Cloneable, Serializable
{
  // FIXME There should be no implicit deepCopy actions in the system, otherwise we loose the
  // ability to apply parallelism. Remove the concept of having multiple outgoing arcs at an
  // output port! Enforce using explicit copy operators!
  public IStreamPacket deepCopy();
}
