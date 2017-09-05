/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.util.Comparator;


public interface DataUtils extends Comparator<Object>
{
  // public static interface Operation
  // {
  // String name(); // implemented by java.lang.Enum
  //    
  // public boolean evalResult(int compareResult);
  // public int compare(DataUtils utils, Object data, SimpleExpression expr);
  // }
  //  
  // public static enum BasicOperation implements Operation
  // {
  // LESS_THAN
  // {
  // public boolean evalResult(int compareResult)
  // {
  // return compareResult < 0;
  // }
  // },
  // EQUAL
  // {
  // public boolean evalResult(int compareResult)
  // {
  // return compareResult == 0;
  // }
  // },
  // GREATER_THAN
  // {
  // public boolean evalResult(int compareResult)
  // {
  // return compareResult > 0;
  // }
  // };
  //    
  // public int compare(DataUtils utils, Object data, SimpleExpression expr)
  // {
  // int comparisonResult = utils.compare(data, expr.constant);
  // return comparisonResult;
  // }
  // }

  public boolean equal(Object o1, Object o2);
  
  public Object add(Object addend1, Object addend2);
  
  public Object subtract(Object minuend, Object subtrahend);
  
  public Object multiply(Object multiplier, Object multiplicant);

  public Object divide(Object dividend, Object devisor);
}
