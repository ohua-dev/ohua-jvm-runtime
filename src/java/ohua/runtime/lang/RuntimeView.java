/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import java.util.List;

import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.lang.operator.AbstractFunctionalOperator;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;

/**
 * This class is meant to provide a view on the currently executing graph.
 * 
 * @author sertel
 * 
 */
public interface RuntimeView
{
  /**
   * Finds all operators in the executing flow graph whose names match the provided regular
   * expression.
   * 
   * @param regex over the operator name
   * @return all operator names that match the regex
   */
  List<String> getAllOperators(String regex);
    
  /**
   * Creates an operator of the defined type.
   * 
   * @param type
   * @return
   * @throws OperatorLoadingException 
   */
  AbstractFunctionalOperator createOperator(String type) throws OperatorLoadingException;

  /**
   * Retrieves the runtime instance of the operator.
   * 
   * @param id - compile-time id of the stateful function call site
   * @return
   */
  OperatorCore findOperator(int id);

  /**
   * Retrieves the runtime instance of the operator.
   * 
   * @param opName - runtime name of the operator
   * @return
   */
  OperatorCore findOperator(String opName);
}
