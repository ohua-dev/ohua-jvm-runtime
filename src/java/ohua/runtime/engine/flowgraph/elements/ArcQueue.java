/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;

public class ArcQueue extends AbstractArcQueue
{
  @Override
  protected Deque<Object> newDataQueue()
  {
    return new LinkedList<>();
  }

  /**
   * All of the operators in com.ohua.lang respect the arc boundary. Therefore, they will not increase the queue size
   * beyond the specified capacity of the arcs in the runtime configuration. It is superior to the LinkedList because it
   * does not create an additional object to store the elements.
   *
   * @param capacity
   * @return
   */
  protected Deque<Object> configureWithMinCapacity(int capacity){
    // leave some head room
    return new ArrayDeque(capacity + 10);
  }

}
