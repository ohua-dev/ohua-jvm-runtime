/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.util.Comparator;
import java.util.logging.Logger;

import ohua.runtime.engine.flowgraph.elements.operator.NotificationBasedOperatorRuntime;
import ohua.runtime.engine.utils.OhuaLoggerFactory;

public class OperatorPriorityComparator implements Comparator<NotificationBasedOperatorRuntime>
{
  private Logger _logger = OhuaLoggerFactory.getLogger(getClass());
  
  public int compare(NotificationBasedOperatorRuntime op1, NotificationBasedOperatorRuntime op2) {
    int result;
    
    // apply rule 1
    result = rule1(op1, op2);
    if(result != 0) {
      return result;
    }
    
    // apply rule 2
    result = rule2(op1, op2);
    return result;
  }
  
  // FIXME verify that rule! we have code in the operator state machine that basically says that
  // we will never activate blocked operators!
  /**
   * Rule 1: Operators that are not being blocked will be positioned closer to the head of the
   * queue.
   * @return
   */
  private int rule1(NotificationBasedOperatorRuntime op1, NotificationBasedOperatorRuntime op2) {
    if(op1.isOperatorBlocked()) {
      if(op2.isOperatorBlocked()) {
        return 0;
      }
      else {
        return 1;
      }
    }
    else {
      if(op2.isOperatorBlocked()) {
        return -1;
      }
      else {
        return 0;
      }
    }
  }
  
  /**
   * Rule 2: Operators with a higher operator priority will be placed closer to the head of the
   * queue.
   * @return
   */
  private int rule2(NotificationBasedOperatorRuntime op1, NotificationBasedOperatorRuntime op2) {
    if(op1.getGraphPriority() > op2.getGraphPriority()) {
      return -1;
    }
    if(op1.getGraphPriority() < op2.getGraphPriority()) {
      return 1;
    }
    
    return 0;
  }
}
