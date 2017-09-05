/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.concurrent.atomic.AtomicInteger;

import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;

public class ProcessID extends AbstractUniqueID
{
  protected ProcessID(int id)
  {
    super(id);
  }

  public static class ProcessIDGenerator
  {
    private static AtomicInteger _processIDCounter = new AtomicInteger(0);
    
    public static ProcessID generateNewProcessID()
    {
      return new ProcessID(_processIDCounter.getAndIncrement());
    }
    
    public static void resetIDCounter()
    {
      _processIDCounter = new AtomicInteger(0);
    }
  }

}
