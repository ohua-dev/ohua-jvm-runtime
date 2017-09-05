/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.List;
import java.util.Set;

public interface PortControl
{
  /**
   * Returns the name of the port managed by this control.
   * @return
   */
  public String getPortName();
  
  /**
   * Serialize the current data packet to a string representation. The format must be supported
   * by the underlying data model implementation.
   * @param format
   * @return
   */
  public String dataToString(String format);
  
  /**
   * Retrieve the names of all the leaf items in the data tree carried by the current data
   * packet.
   * @return
   */
  public Set<String> getLeafs();
  
  /**
   * A function to retrieve data from the data structure inside the current packet.
   * @param path
   * @return
   */
  public List<Object> getData(String path);
  
  public Object getData();
  
  /**
   * Set data into the data structure carried along by the current packet.
   * @param path
   * @param value
   */
  public void setData(String path, Object value);
}
