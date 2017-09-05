/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

public interface DataFormat
{
  public DataPacket createDataPacket();
  
  public OhuaDataAccessor createDataAccessor();
  
  public DataUtils getComparisonUtils();
}
