/*
 * Copyright (c) Sebastian Ertel 2011. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public interface Statistics
{
  public void setStatisticsConfig(String statisticsConfig);
  
  public void recordFlowStatistics(File outFile, String... columns) throws IOException;
  
  public void recordFlowStatistics(String... columns) throws FileNotFoundException, IOException;
  
  public void recordFlowStatistics() throws FileNotFoundException, IOException;
  
  public void record(String key, long runtime);
  
  public void recordAVG(String key, long value);
  
  public void recordDiff(String key, long value);
  
  public void recordAdd(String key, long value);
  
  public void recordMAX(String key, long value);
  
  public void recordDistance(String key, long value);

  public void recordACC(String string, long value);

  public Object getStatistic(String string);

}
