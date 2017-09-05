/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;import java.util.Comparator;

import ohua.runtime.engine.points.IPrioritizedPacketHandler;
import ohua.runtime.engine.points.InputPortEvents;

public class PriorityComparator implements Comparator<IPrioritizedPacketHandler>
{
  private InputPortEvents _event = null;
  
  public PriorityComparator(InputPortEvents event)
  {
    _event = event;
  }

  /**
   * This orders highest first.
   */
  public int compare(IPrioritizedPacketHandler o1, IPrioritizedPacketHandler o2)
  {
    return o2.getPriority(_event).level() - o1.getPriority(_event).level();
  }
}
