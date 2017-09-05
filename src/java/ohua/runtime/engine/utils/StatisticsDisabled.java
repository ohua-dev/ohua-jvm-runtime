/*
 * Copyright (c) Sebastian Ertel 2011. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class StatisticsDisabled implements Statistics
{
  public void setStatisticsConfig(String statisticsConfig)
  {
    // nothing
  }
  
  public void recordFlowStatistics(File outFile, String... columns) throws IOException
  {
    // nothing
  }
  
  public void recordFlowStatistics(String... columns) throws FileNotFoundException, IOException
  {
    // nothing
  }
  
  public void recordFlowStatistics() throws FileNotFoundException, IOException
  {
    // nothing
  }
  
  public void record(String key, long runtime)
  {
    // nothing
  }
  
  public void recordAVG(String key, long value)
  {
    // nothing
  }
  
  public void recordDiff(String key, long value)
  {
    // nothing
  }
  
  public void recordAdd(String key, long value)
  {
    // nothing
  }
  
  public void recordMAX(String key, long value)
  {
    // nothing
  }
  
  public void recordDistance(String key, long value)
  {
    // nothing
  }

  public void recordACC(String string, long value)
  {
    // nothing
  }

  @Override
  public Object getStatistic(String string)
  {
    // TODO Auto-generated method stub
    return null;
  }

}

