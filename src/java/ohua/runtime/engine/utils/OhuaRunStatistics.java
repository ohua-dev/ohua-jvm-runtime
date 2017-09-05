package ohua.runtime.engine.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class OhuaRunStatistics extends AbstractStatistics
{
  private static OhuaRunStatistics _stats = new OhuaRunStatistics();
  
  public static OhuaRunStatistics getInstance()
  {
    return _stats;
  }

  public static class RunStatistic
  {
    public String _id = null;
    public List<String[]> _values = new ArrayList<String[]>();
  }

  private ConcurrentHashMap<String, RunStatistic> _runStats =
      new ConcurrentHashMap<String, RunStatistic>();

  public void registerRunStatistic(RunStatistic runStat)
  {
    _runStats.put(runStat._id, runStat);
  }
  
  public void recordFlowStatistics() throws IOException
  {
    Properties props = super.retrieveStatsConfig();
    if(!props.containsKey("root-output-dir"))
    {
      System.err.println("Missing root output dir. No run statistics recorded!");
      return;
    }

    String runOutputDir = props.getProperty("root-output-dir");

    for(Map.Entry<String, RunStatistic> entry : _runStats.entrySet())
    {
      File outputFile = new File(runOutputDir + entry.getValue()._id + ".dat");
      FileWriter writer = new FileWriter(outputFile);
      for(String[] runEntry : entry.getValue()._values)
      {
        super.writeRecord(writer, runEntry);
      }
      writer.flush();
      writer.close();
    }
  }

}
