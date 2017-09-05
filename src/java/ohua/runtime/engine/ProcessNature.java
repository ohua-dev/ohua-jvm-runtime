/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;


public enum ProcessNature
{
  /**
   * None of the external source produces an infinite amount of data and therefore the
   * computation can be defined as complete when all data from all of the external sources has
   * been processed.
   */
  SOURCE_DRIVEN,
  
  /**
   * This means that there exists at least one external input to the flow that potentially
   * produces an infinite amount of data. For that case the user is required to steer the flow
   * and decide when to shut it down.
   */
  USER_DRIVEN;
}
