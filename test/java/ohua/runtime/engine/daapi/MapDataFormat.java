/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.daapi;

public class MapDataFormat implements DataFormat
{
  @Override
  public DataPacket createDataPacket()
  {
    return new MapDataPacket();
  }
  
  @Override
  public OhuaDataAccessor createDataAccessor()
  {
    return new MapDataAccessor();
  }
  
  @Override
  public DataUtils getComparisonUtils()
  {
    return new MapDataUtils();
  }
}
