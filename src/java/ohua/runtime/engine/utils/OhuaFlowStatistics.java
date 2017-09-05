/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OhuaFlowStatistics extends AbstractStatistics implements Statistics
{
  interface Computable
  {
    void add(Long value);
    
    long compute();
  }
  
  class ADD implements Computable
  {
    private AtomicLong _add = new AtomicLong(0);
    
    public void add(Long value)
    {
      _add.addAndGet(value);
    }
    
    public long compute()
    {
      return _add.get();
    }
  }

  class AVG implements Computable
  {
    private long _sum = 0;
    private int _count = 0;
    
    public void add(Long value)
    {
      _sum = _sum + value;
      _count++;
    }
    
    public long compute()
    {
      if(_count == 0)
      {
        return 0;
      }
      else
      {
        long round = Math.round((double) (_sum / _count));
        System.out.println("Calculating: " + _sum + "/" + _count + " = " + round);
        return round;
      }
    }
  }
  
  class MaxDistance implements Computable
  {
    private Long _start = 0L;
    private AtomicLong _end = new AtomicLong(0);

    public void add(Long value)
    {
      if(_start == 0)
      {
        _start = value;
      }
      else
      {
        _end.set(value);
      }
    }
    
    public long compute()
    {
      return _end.get() - _start;
    }
  }
  
  class ACC implements Computable
  {
    private List<Long> _acc = new ArrayList<Long>();
    private int _outputIndex = 0;
    
    public void add(Long value)
    {
      _acc.add(value);
    }
    
    public int recordCount()
    {
      return _acc.size();
    }

    public long compute()
    {
      if(_outputIndex < _acc.size())
      {
        return _acc.get(_outputIndex);
      }
      else
      {
        return 0;
      }
    }
  }

   private static Statistics _instance = new StatisticsDisabled();
//  private static Statistics _instance = new OhuaFlowStatistics();
  
  public static Statistics getInstance()
  {
    return _instance;
  }
  
  public OhuaFlowStatistics()
  {
    // singleton
  }
  
  private ConcurrentHashMap<String, Object> _recordRegistry =
      new ConcurrentHashMap<String, Object>();
  private ConcurrentHashMap<String, AtomicInteger> _updateCount =
      new ConcurrentHashMap<String, AtomicInteger>();
  
  @Override
  public void setStatisticsConfig(String statisticsConfig)
  {
    super.setStatisticsConfig(statisticsConfig);
    
    Properties props = retrieveStatsConfig();
    
    String colStr = props.getProperty("update-counts");
    if(colStr == null)
    {
      return;
    }
    String[] columns = colStr.split("\\|");
    for(int i = 0; i < columns.length; i++)
    {
      _updateCount.put(columns[i],
                       new AtomicInteger(Integer.parseInt(props.getProperty(columns[i]
                                                                            + "-update-count"))));
    }
  }

  public void recordFlowStatistics(File outFile, String... columns) throws IOException
  {
    if(!outFile.exists())
    {
      prepareOutput(outFile, columns);
    }
    
    // create one entry with the requested columns
    appendOutput(outFile, _recordRegistry, columns);
  }
  
  public final void recordFlowStatistics(String... columns) throws FileNotFoundException,
                                                           IOException
  {
    Properties props = retrieveStatsConfig();
    
    String filePath = props.getProperty("file");
    recordFlowStatistics(new File(filePath), columns);
  }

  public final void recordFlowStatistics() throws FileNotFoundException, IOException
  {
    Properties props = retrieveStatsConfig();
    
    String filePath = props.getProperty("file");
    String colStr = props.getProperty("columns");
    String[] columns = colStr.split("\\|");
    for(int i = 0; i < columns.length; i++)
    {
      columns[i] = columns[i].trim();
    }
    for(String column : columns)
    {
      if(props.containsKey(column))
      {
        // handle default values
        record(column, Long.parseLong(props.getProperty(column)));
      }
    }

    int loops = findMaxAccs(columns);
    for(int i = 0; i < loops; i++)
    {
      recordFlowStatistics(new File(filePath), columns);
    }
  }

  private int findMaxAccs(String[] columns)
  {
    int max = 1;
    for(String column : columns)
    {
      Object o = _recordRegistry.get(column);
      if(o instanceof ACC)
      {
        max = Math.max(((ACC) o).recordCount(), max);
      }
    }
    return max;
  }

  public void record(String key, long runtime)
  {
    _recordRegistry.put(key, runtime);
  }
  
  public void recordAVG(String key, long value)
  {
    if(!_recordRegistry.containsKey(key))
    {
      Computable val = new AVG();
      _recordRegistry.put(key, val);
    }
    
    Computable oldValue = (Computable) _recordRegistry.get(key);
    oldValue.add(value);
  }
  
  public void recordACC(String key, long value)
  {
    if(!_recordRegistry.containsKey(key))
    {
      Computable val = new ACC();
      _recordRegistry.put(key, val);
    }
    
    Computable oldValue = (Computable) _recordRegistry.get(key);
    oldValue.add(value);
  }

  public void recordDiff(String key, long value)
  {
    if(!_recordRegistry.containsKey(key))
    {
      record(key, value);
    }
    else
    {
      long oldValue = (Long) _recordRegistry.get(key);
      _recordRegistry.put(key, (value - oldValue));
    }
  }
  
  public void recordAdd(String key, long value)
  {
    if(!_recordRegistry.containsKey(key))
    {
      Computable val = new ADD();
      _recordRegistry.putIfAbsent(key, val);
    }
    
    Computable oldValue = (Computable) _recordRegistry.get(key);
    oldValue.add(value);

  }
  
  public void recordMAX(String key, long value)
  {
    if(!_recordRegistry.containsKey(key))
    {
      record(key, value);
    }
    else
    {
      long oldValue = (Long) _recordRegistry.get(key);
      if(oldValue < value)
      {
        _recordRegistry.put(key, value);
      }
    }
  }

  public void recordDistance(String key, long value)
  {
    _recordRegistry.putIfAbsent(key, new MaxDistance());
    
    Computable oldValue = (Computable) _recordRegistry.get(key);
    oldValue.add(value);
    
    if(_updateCount.containsKey(key))
    {
      AtomicInteger count = _updateCount.get(key);
      int current = count.decrementAndGet();
      if(current == 0)
      {
        try
        {
          recordFlowStatistics();
        }
        catch(Exception e)
        {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Object getStatistic(String string)
  {
    return _recordRegistry.get(string);
  }
}
