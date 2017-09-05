package ohua.runtime.engine.utils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import ohua.runtime.engine.utils.OhuaFlowStatistics.Computable;

public abstract class AbstractStatistics
{
  
  private String _statisticsConfig = null;

  public void setStatisticsConfig(String statisticsConfig)
  {
    _statisticsConfig = statisticsConfig;
  }
  
  protected Properties retrieveStatsConfig()
  {
    Properties props = new Properties();
    try
    {
      FileReader reader = new FileReader(_statisticsConfig);
      props.load(reader);
      reader.close();
    }
    catch(Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return props;
  }

  protected void appendOutput(File outFile, Map<String, Object> records, String... columns) throws IOException
  {
    FileWriter writer = new FileWriter(outFile, true);
    
    StringBuilder builder = new StringBuilder();
    
    Long[] times = new Long[columns.length];
    int i = 1;
    for(String column : columns)
    {
      int length = column.length() > 20 ? column.length()
                                       : 20;
      System.out.println("column exists: " + records.containsKey(column));
      if(!records.containsKey(column))
      {
        System.out.println("keys: " + Arrays.deepToString(records.keySet().toArray()));
      }
      System.out.println("column: " + column + " _recordsRegistry.get(column): "
                         + records.get(column));
      times[i - 1] =
          records.get(column) instanceof Computable ? ((Computable) records.get(column)).compute()
                                                   : (Long) records.get(column);
      builder.append("%" + i + "$" + length + "d|");
      i++;
    }
    builder.append("\n");
    String row = String.format(builder.toString(), (Object[]) times);
    writer.write(row);
    writer.close();
  }
  
  protected void prepareOutput(File outFile, String... columns) throws IOException
  {
    FileWriter writer = new FileWriter(outFile);
    
    StringBuilder builder = new StringBuilder();
    StringBuilder line = new StringBuilder();
    builder.append("#");
    line.append("#");
    int i = 1;
    for(String column : columns)
    {
      int length = column.length() > 20 ? column.length()
                                       : 20;
      builder.append("%" + i++ + "$-" + length + "s|");
      for(int j = 0; j <= length; j++)
      {
        line.append("-");
      }
    }
    builder.append("\n");
    line.append("\n");
    String row = String.format(builder.toString(), (Object[]) columns);
    writer.write(row);
    writer.write(line.toString());
    
    writer.close();
  }
  
  protected void writeRecord(FileWriter writer, String... columns) throws IOException
  {
    StringBuilder builder = new StringBuilder();
    int i = 1;
    for(String column : columns)
    {
      int length = column.length() > 20 ? column.length()
                                       : 20;
      builder.append("%" + i++ + "$-" + length + "s|");
    }
    builder.append("\n");
    String row = String.format(builder.toString(), (Object[]) columns);
    writer.write(row);
  }

}
