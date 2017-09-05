/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements;


public abstract class ScopedUniqueID extends AbstractUniqueID
{
  private AbstractUniqueID _scope = null;

  public ScopedUniqueID(int id)
  {
    super(id);
  }

  public void associate(AbstractUniqueID scope)
  {
    _scope = scope;
  }
  
  public AbstractUniqueID getScope()
  {
    return _scope;
  }

  public boolean isInScope(AbstractUniqueID id)
  {
    return _scope != null ? _scope.equals(id)
                         : true;
  }

  @Override
  public String toString()
  {
    String s = super.toString();
    return _scope != null ? _scope.toString() + ":" + s
                                 : s;
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if(this == obj)
      return true;
    if((obj == null) || (obj.getClass() != this.getClass()))
      return false;
    
    // at this point it must be a ScopedUniqueID as well
    ScopedUniqueID other = (ScopedUniqueID) obj;
    boolean sameProcess = _scope != null ? _scope.equals(other._scope)
                                                : other._scope == null;
    
    return sameProcess ? super.equals(other)
                      : false;
  }
  
  @Override
  public int hashCode()
  {
    if(_scope != null)
    {
    int hash = 7;
    hash = 31 * hash + _scope.hashCode();
    hash = 31 * hash + super.hashCode();
      return hash;
    }
    else
    {
      return super.hashCode();
    }
  }

}
