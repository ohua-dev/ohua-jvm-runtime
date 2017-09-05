/*
 * Copyright � Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.io.File;

public interface OutputPortControl extends PortControl
{
  /**
   * Send the current packet downstream.
   * @return true means return control to the framework
   */
  boolean send();
  
  /**
   * Create a new packet from the packet template (formal output schema) set to this output
   * port.
   */
  void newPacket();
  
  /**
   * Load a packet template (formal output schema) from a file.
   * @param file
   */
  void load(File file);
  
  /**
   * Parse data from a String.
   * @param data
   */
  void parse(String data, String format);
  
}
