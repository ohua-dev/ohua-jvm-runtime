/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import java.io.Serializable;

import ohua.runtime.engine.flowgraph.elements.AbstractUniqueID;


public class PortID extends AbstractUniqueID implements Serializable
{
  public PortID(int id)
  {
    super(id);
  }
  
  public static class PortIDGenerator
  {
    private static int _portIDcounter = 0;
    
    public static PortID generateNewPortID()
    {
      return new PortID(_portIDcounter++);
    }
    
    public static void resetCounter()
    {
      _portIDcounter = 0;
    }

  }
  
}
