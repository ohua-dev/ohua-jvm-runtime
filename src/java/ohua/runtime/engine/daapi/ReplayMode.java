/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

public enum ReplayMode
{
  BUFFER
  {
    @Override
    public PacketCursor getReplayer(InputPortController controller)
    {
      return controller._bufferSupport;
    }
  },
  HEAP
  {
    @Override
    public PacketCursor getReplayer(InputPortController controller)
    {
      return controller._packetHeap;
    }
  };
  
  abstract public PacketCursor getReplayer(InputPortController controller);

}
