/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.Serializable;
import java.util.Properties;
import java.util.Deque;

import ohua.runtime.engine.daapi.DataAccess;
import ohua.runtime.engine.daapi.DataAccessLayer;
import ohua.runtime.engine.daapi.DataFormat;
import ohua.runtime.engine.flowgraph.elements.operator.AbstractOperatorRuntime;
import ohua.runtime.lang.operator.Stats;
import ohua.runtime.engine.sections.AbstractSectionGraphBuilder;
import ohua.runtime.engine.sections.OneOpOneSectionGraphBuilder;

public class RuntimeProcessConfiguration implements Serializable {
  
  public static boolean LOGGING_ENABLED = false;
  // TODO compact to a single var.
  public static Class<? extends Stats.IStatsCollector> STATS_COLLECTOR = Stats.Disabled.class;
  public static Class<? extends Stats.IStatsCollector> FRAMEWORK_STATS_COLLECTOR = null;
  public static Class<? extends Stats.IStatsCollector> FUNCTION_STATS_COLLECTOR = null;

  public enum Parallelism {
    SINGLE_THREADED,
    MULTI_THREADED
  }

  public enum BuiltinProperties{
    ARC_BOUNDARY("arc-boundary", 200),
    CORE_THREAD_POOL_SIZE("core-thread-pool-size", 10),
    EXECUTION_MODE("execution-mode", Parallelism.SINGLE_THREADED),
    RUNTIME("runtime", NotificationBasedRuntime.class),
    SECTION_STRATEGY("section-strategy", OneOpOneSectionGraphBuilder.class);

    private String _key;
    private Object _defaultVal;
    BuiltinProperties(String key, Object defaultVal){
      _key = key;
      _defaultVal = defaultVal;
    }

    public String getKey(){
      return _key;
    }

    private Object getDefault(){
      return _defaultVal;
    }
  }

  public static <T> T throwExcept(Object arg){
    throw new IllegalArgumentException("Arg '" + arg + "' has wrong type '" + arg.getClass() + "'.");
  }

  protected Properties _properties = new Properties();

  public void setProperties(Properties properties) {
    _properties = properties;
  }
  
  public AbstractProcessManager getProcessManager(DataFlowProcess process) {
    String processManagerClass =
        _properties.getProperty("process-manager", "ohua.runtime.engine.OhuaProcessManager").trim();
    try {
      // LOAD_RESOURCES = false;
      return (AbstractProcessManager) Class.forName(processManagerClass).getConstructor(DataFlowProcess.class,
                                                                                        RuntimeProcessConfiguration.class).newInstance(process,
                                                                                                                                       this);
    }
    catch(Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  public AbstractSectionGraphBuilder getSectionStrategy() {
    Object stratgeyClz = _properties.get(BuiltinProperties.SECTION_STRATEGY.getKey());
    try {
      return stratgeyClz == null ?
              ((Class<? extends AbstractSectionGraphBuilder>) BuiltinProperties.SECTION_STRATEGY.getDefault()).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
              (stratgeyClz instanceof AbstractSectionGraphBuilder) ?
                      (AbstractSectionGraphBuilder) stratgeyClz :
                      (stratgeyClz instanceof Class) ?
                              ((Class<? extends AbstractSectionGraphBuilder>) stratgeyClz).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
                              (stratgeyClz instanceof String) ?
                                      (AbstractSectionGraphBuilder) Class.forName(((String) stratgeyClz).trim()).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
                                      throwExcept(stratgeyClz);
    } catch(Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  private int getArcBoundary() {
    Object arcBoundary = _properties.get(BuiltinProperties.ARC_BOUNDARY.getKey());
    return arcBoundary == null ?
            (int) BuiltinProperties.ARC_BOUNDARY.getDefault() :
            (arcBoundary instanceof Integer) ?
                    (int) arcBoundary :
                    (arcBoundary instanceof String) ?
                            Integer.parseInt(((String) arcBoundary).trim()) :
                            throwExcept(arcBoundary);
  }

  public int getArcEnqueueWatermark() {
    return Integer.parseInt(_properties.getProperty("arc-enqueue-watermark", "-1").trim());
  }
  
  public int getInterSectionArcBoundary() {
    if(_properties.containsKey("inter-section-arc-boundary")) {
      return Integer.parseInt(_properties.getProperty("inter-section-arc-boundary", "200").trim());
    } else {
      return getArcBoundary();
    }
  }
  
  public int getInnerSectionArcBoundary() {
    if(_properties.containsKey("inner-section-arc-boundary")) {
      return Integer.parseInt(_properties.getProperty("inner-section-arc-boundary", "200").trim());
    } else {
      return getArcBoundary();
    }
  }
  
  @SuppressWarnings({ "unchecked",
                     "rawtypes" })
  public Class<? extends Deque> getInterSectionQueueImpl() {
    String impl = _properties.getProperty("inter-section-queue",
                                          "ohua.runtime.engine.flowgraph.elements.ConcurrentArcQueue$OhuaConcurrentLinkedDeque");
    if(impl == null) return null;
    else {
      try {
        return (Class<? extends Deque>) Class.forName(impl.trim());
      }
      catch(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
  
  @SuppressWarnings({ "unchecked",
                     "rawtypes" })
  public Class<? extends Deque> getInterSectionLowLatencyQueueImpl() {
    String impl = _properties.getProperty("low-latency-queue");
    if(impl == null) return null;
    else {
      try {
        return (Class<? extends Deque>) Class.forName(impl.trim());
      }
      catch(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
  
  public Parallelism getExecutionMode() {
    Object execMode = _properties.get(BuiltinProperties.EXECUTION_MODE.getKey());
    return execMode == null ?
            (Parallelism) BuiltinProperties.EXECUTION_MODE.getDefault() :
            (execMode instanceof Parallelism) ?
                    (Parallelism) execMode :
                    (execMode instanceof String) ?
                            Parallelism.valueOf(((String)execMode).trim()) :
                            throwExcept(execMode);
  }
  
  public int getCoreThreadPoolSize() {
    Object coreThreadPoolSize = _properties.get(BuiltinProperties.CORE_THREAD_POOL_SIZE.getKey());
    return coreThreadPoolSize == null ?
            (int) BuiltinProperties.CORE_THREAD_POOL_SIZE.getDefault() :
            (coreThreadPoolSize instanceof Integer) ?
                    (int) coreThreadPoolSize :
                    (coreThreadPoolSize instanceof String) ?
                            Integer.parseInt(((String) coreThreadPoolSize).trim()) :
                            throwExcept(coreThreadPoolSize);
  }
  
  public int getMaxThreadPoolSize() {
    return Integer.parseInt(_properties.getProperty("max-thread-pool-size", "10").trim());
  }
  
  public DataFormat getDataFormat() {
    return (DataFormat) _properties.get("data-format");
  }
  
  public void setCoreThreadPoolSize(int i) {
    _properties.setProperty("core-thread-pool-size", Integer.toString(i));
  }
  
  public void setMaxThreadPoolSize(int i) {
    _properties.setProperty("max-thread-pool-size", Integer.toString(i));
  }
  
  public void aquirePropertiesAccess(ConfigurationExtension extension) {
    extension.setProperties(_properties);
  }
  
  public int getSchedulingQuanta() {
    return Integer.parseInt(_properties.getProperty("scheduling-quanta", "50").trim());
  }
  
  public int getSectionSize() {
    return Integer.parseInt(_properties.getProperty("section-size", "2").trim());
  }

  public int getOperatorQuanta() {
    return Integer.parseInt(_properties.getProperty("operator-quanta", "1000").trim());
  }
  
  public int getArcActivationMark() {
    return Integer.parseInt(_properties.getProperty("arc-activation", "-1").trim());
  }
  
  public Object getArcConfiguration() {
    return _properties.get("arc-configuration");
  }
  
  public AbstractRuntime getRuntime() {
    Object schedulerClz = _properties.get(BuiltinProperties.RUNTIME.getKey());
    try {
      return schedulerClz == null ?
              ((Class<? extends AbstractRuntime>) BuiltinProperties.RUNTIME.getDefault()).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
              (schedulerClz instanceof Class) ?
                      ((Class<? extends AbstractRuntime>) schedulerClz).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
                      (schedulerClz instanceof String) ?
                              (AbstractRuntime) Class.forName(((String) schedulerClz).trim()).getDeclaredConstructor(RuntimeProcessConfiguration.class).newInstance(this) :
                              throwExcept(schedulerClz);
    } catch(Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public boolean isLoggingEnabled() {
    return Boolean.parseBoolean(_properties.getProperty("logging-enabled", "false").trim());
  }

  public boolean isConcurrentSchedulingEnabled() {
    return Boolean.parseBoolean(_properties.getProperty("concurrent-scheduling-enabled", "false").trim());
  }

  public DataAccess getDataAccess(AbstractOperatorRuntime op, DataFormat dataFormat){
    String dataAccessClass = _properties.getProperty("data-access", DataAccessLayer.class.getName()).trim();
    try {
      return (DataAccess) Class.forName(dataAccessClass).getConstructor(AbstractOperatorRuntime.class, DataFormat.class).newInstance(op, dataFormat);
    }
    catch(Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public String toString(){
    return _properties.toString();
  }
  
}
