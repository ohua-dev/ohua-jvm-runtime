/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.Map;

import ohua.runtime.engine.exceptions.LibraryLoadingException;

/**
 * The interface for the operator store.
 * 
 * @author sertel
 *
 */
public interface OperatorStore
{ 
  public Map<String, String> load() throws LibraryLoadingException;
  public void store(Map<String, String> operators);
}
