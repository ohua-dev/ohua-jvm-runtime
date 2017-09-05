/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;


public interface PacketBuffer
{
  /**
   * Buffers the current packet.
   */
  public void buffer();

  /**
   * Reset the cursor back to position 0.
   */
  public void reset();
  
  /**
   * Interrupt a replay. The next invocation of next() will retrieve a new packet from the input
   * port.
   */
  public void stop();
  
  /**
   * Clears the buffer.
   */
  public void clear();
}
