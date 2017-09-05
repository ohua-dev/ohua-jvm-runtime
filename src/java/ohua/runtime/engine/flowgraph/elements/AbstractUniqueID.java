/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;

import java.io.Serializable;

public abstract class AbstractUniqueID implements Serializable
{
  private int _id = -1;
  
  public AbstractUniqueID(int id)
  {
    _id = id;
  }
  
  public int getIDInt()
  {
    return _id;
  }
  
  @Override
  public String toString()
  {
    return Integer.toString(_id);
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if(this == obj)
      return true;
    if((obj == null) || (obj.getClass() != this.getClass()))
      return false;
    
    // object must be AbstractUniqueID at this point
    AbstractUniqueID test = (AbstractUniqueID) obj;
    return _id == test.getIDInt(); 
  }
  
  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 31 * hash + _id;
    
    return hash;
  }
  
  public int compareTo(AbstractUniqueID uniqueID)
  {
    return _id - uniqueID._id;
  }
}
