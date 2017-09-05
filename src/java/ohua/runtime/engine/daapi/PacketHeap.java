/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

public interface PacketHeap
{
  /**
   * Stores the current item on the input port in the heap. It can be later on accessed using
   * the given key.
   * @param key
   */
  public void store(String key);
  
  /**
   * Finds all items matching the given key and allows them to be replayed using the
   * PacketBuffer API.
   * @param key
   */
  public void query(String key);
  
  /**
   * Deletes all the saved heap items.
   */
  public void clean();
}
