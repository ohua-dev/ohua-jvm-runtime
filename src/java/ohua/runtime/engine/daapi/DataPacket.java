/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.daapi;

import ohua.runtime.engine.flowgraph.elements.packets.IStreamPacket;

public interface DataPacket extends IStreamPacket
{
  public DataPacket deepCopy();
  
  public String serialize();
  
  public void deserialize(String data);
  
  public Object getData();
  
  public void setData(Object dataRoot);
}
