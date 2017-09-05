/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.packets.functionality;

public enum PriorityLevel
{
  FIRST
  {
    @Override
    public int level()
    {
      return 100;
    }
  },
  HIGH
  {
    @Override
    public int level()
    {
      return 75;
    }
  },
  MEDIUM
  {
    @Override
    public int level()
    {
      return 50;
    }
  },
  LOW
  {
    @Override
    public int level()
    {
      return 25;
    }
  },
  LAST

  {
    @Override
    public int level()
    {
      return 0;
    }
  };

  public abstract int level();
}
