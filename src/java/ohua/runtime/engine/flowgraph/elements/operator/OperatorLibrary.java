/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.HashMap;
import java.util.Map;

import ohua.runtime.engine.exceptions.LibraryLoadingException;
import ohua.runtime.engine.exceptions.LibraryLoadingException.LIB_LOADING_FAILURE;

public abstract class OperatorLibrary
{
  public static Map<String, String> loadLibrary(String lib) throws Exception {
    XMLOperatorStore store = new XMLOperatorStore(lib);
    return store.load();
  }
  
  public static void registerOperators(Map<String, String> newOps) throws Exception {
    registerOperators(newOps, "META-INF/operators/registry.xml");
  }
  
  public static void registerOperators(Map<String, String> newOps, String registry) throws Exception {
    XMLOperatorStore store = new XMLOperatorStore(registry);
    Map<String, String> operatorLib = null;
    try {
      operatorLib = store.load();
    }
    catch(LibraryLoadingException e) {
      if(e.getCauze() == LIB_LOADING_FAILURE.LIB_NOT_FOUND) {
        // it does not exist yet. no problem, we will just create a new one.
        operatorLib = new HashMap<String, String>();
      }
      else {
        throw e;
      }
    }
    operatorLib.putAll(newOps);
    store.store(operatorLib);
  }
  
}
