/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import java.util.HashMap;
import java.util.Map;

/**
 * This class should allow us to control the execution of all Ohua parts inside the Clojure
 * context/program.
 * 
 * @author sertel
 * 
 */
public class OhuaRuntimeRegistry
{
  private static OhuaRuntimeRegistry _reg = new OhuaRuntimeRegistry();
  
  public static OhuaRuntimeRegistry get() {
    return _reg;
  }
  
  private Map<String, OhuaRuntime> _runtimes = new HashMap<>();
  
  private OhuaRuntimeRegistry() {
    // singleton
  }
  
  public void register(String id, OhuaRuntime runtime) {
    _runtimes.put(id, runtime);
  }
  
  public OhuaRuntime getRuntime(String id) {
    return _runtimes.get(id);
  }
}
