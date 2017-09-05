/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.flowgraph.elements.ScopedUniqueID;


public class OperatorID extends ScopedUniqueID
{
  public OperatorID(int id)
  {
    super(id);
  }

  // Please note that this guy is actually almost never being used because we take the ids
  // stated by the flow author.
  public static class OperatorIDGenerator
  {
    private static int _operatorIDCounter = 0;
    
    public static OperatorID generateNewOperatorID()
    {
      return new OperatorID(_operatorIDCounter++);
    }
    
    public static void resetCounter()
    {
      _operatorIDCounter = 0;
    }
    
    public static void setStartValue(int startValue)
    {
      _operatorIDCounter = startValue;
    }
  }
}
