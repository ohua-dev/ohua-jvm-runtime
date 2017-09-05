/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.io.FileNotFoundException;
import java.util.Map;

import ohua.runtime.engine.exceptions.LibraryLoadingException;
import ohua.runtime.engine.exceptions.LibraryLoadingException.LIB_LOADING_FAILURE;
import ohua.runtime.engine.exceptions.XMLParserException;
import ohua.runtime.engine.utils.parser.OperatorMappingParser;

public class XMLOperatorStore implements OperatorStore
{ 
  private OperatorMappingParser _parser = new OperatorMappingParser();
  private String _registry = null;
  
  public XMLOperatorStore(String registry){
    _registry = registry;
  }
  
  public Map<String, String> load() throws LibraryLoadingException{
    try {
      return _parser.loadOperators(_registry);
    }
    catch(XMLParserException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(FileNotFoundException e) {
      throw new LibraryLoadingException(LIB_LOADING_FAILURE.LIB_NOT_FOUND);
    }
  }
  
  public void store(Map<String, String> operators){
    OperatorMappingSerializer.store(operators, _registry);
  }
}
