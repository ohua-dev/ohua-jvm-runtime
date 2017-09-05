/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.points;

import ohua.runtime.engine.flowgraph.elements.operator.InputPort;

// FIXME those guys are being used by the framework in HashMaps so they need a key and need to implement their own equals() and hashCode()!
public interface IPacketHandler
{
  /**
   * This function will tell the handler on how many ports it has to wait until it can propagate
   * the marker!
   * @param port The input port from which this handler will receive call backs from.
   */
  public void addCallback(InputPortEvents event, InputPort port);
  
  public void removeCallback(InputPortEvents event, InputPort port);
  
  /**
   * WARNING: Please note that the implementation of this function is called during
   * initialization of each port! If the handler is registered on multiple ports and multiple
   * visitors then the implementation must take care of that and may need to implement
   * idempotency!
   */
  // FIXME This function needs two parameters: the visitor and the port in order to get rid of
  // the requirement above! Or better: call this function only once after all handlers have been
  // registered.
  public void init();
}
