/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.RuntimeProcessConfiguration;

import java.io.IOException;

public abstract class Stats {
  
  public interface IStatsCollector {

    void begin();
    
    void end();
    
    void log(Appendable resource) throws IOException;
  }
  
  public static class Disabled implements IStatsCollector {

    @Override
    public void begin() {
      // nothing
    }
    
    @Override
    public void end() {
      // nothing
    }
    
    @Override
    public void log(Appendable resource) throws IOException {
      // just here to produce valid JSON data
      resource.append("[]");
    }
  }

  public interface ILoggable{
    void logStats(Appendable resoure) throws IOException;
  }

  public static Stats.IStatsCollector createStatsCollector() {
    return createStatsCollector(RuntimeProcessConfiguration.STATS_COLLECTOR);
  }

  public static Stats.IStatsCollector createStatsCollector(Class<? extends Stats.IStatsCollector> collectorClass) {
    try {
      return collectorClass.newInstance();
    } catch (Throwable t) {
      System.err.println("Caught the following exception during statistics setup: (swallowing - disabling stats logger)");
      System.err.println(t.getMessage());
      return new Stats.Disabled();
    }
  }

  public static Stats.IStatsCollector createFrameworkStatsCollector() {
    if(RuntimeProcessConfiguration.FRAMEWORK_STATS_COLLECTOR == null)
      return createStatsCollector(RuntimeProcessConfiguration.STATS_COLLECTOR);
    else
      return createStatsCollector(RuntimeProcessConfiguration.FRAMEWORK_STATS_COLLECTOR);
  }

  public static Stats.IStatsCollector createFunctionStatsCollector() {
    if(RuntimeProcessConfiguration.FUNCTION_STATS_COLLECTOR == null)
      return createStatsCollector(RuntimeProcessConfiguration.STATS_COLLECTOR);
    else
      return createStatsCollector(RuntimeProcessConfiguration.FUNCTION_STATS_COLLECTOR);
  }

}
