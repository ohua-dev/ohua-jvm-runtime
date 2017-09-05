/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;


public class ArcID extends AbstractUniqueID
{
  public ArcID(int id)
  {
    super(id);
  }
  
  public static class ArcIDGenerator
  {
    private static int _arcIDcounter = 0;
    
    public static ArcID generateNewArcID()
    {
      return new ArcID(_arcIDcounter++);
    }
    
    public static void resetCounter()
    {
      _arcIDcounter = 0;
    }
  }

}
