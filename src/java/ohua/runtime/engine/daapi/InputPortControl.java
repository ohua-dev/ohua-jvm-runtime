/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;



public interface InputPortControl extends PacketBuffer, PortControl, PacketHeap
{
  /**
   * Replays the buffered packets. The replay ends when either stop() has been called or end of
   * the buffer has been reached.
   * <p>
   * It is possible to replay a buffer unlimited amount of times.
   * <p>
   * The replay starts either from the first packet in the buffer or from the position where the
   * last replay has been interrupted. (stop() was called.)
   */
  void replay(ReplayMode mode);
  
  /**
   * Indicates whether more data will arrive in the future among this input port.
   * @return
   */
  boolean hasSeenLastPacket();
  
  /**
   * Retrieve a new packet and stores it as its current packet ready for data access thereby
   * deleting the previous one.
   * @return
   */
  boolean next();

  /**
   *
   * @return true, if data packet is still buffered in controller or data is available among incoming arc.
   */
  boolean hasData();

}
