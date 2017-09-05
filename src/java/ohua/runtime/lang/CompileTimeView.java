/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.util.Tuple;

import java.util.List;

/**
 * Provides information that is statically encoded into the clojure algorithm such as data
 * dependencies, formal and actual schema information and arguments.
 * 
 * @author sertel
 * 
 */
public interface CompileTimeView {
  int extractIDFromRef(String operatorRef);
  
  List<Object[]> getInputSchema(int opID);
  
  List<int[]> getOutputSchema(int opID);
  
  Tuple<Integer, Object>[] getArguments(int opID);
  
}
