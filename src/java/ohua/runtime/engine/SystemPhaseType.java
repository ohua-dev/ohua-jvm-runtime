/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.Serializable;

public enum SystemPhaseType implements Serializable
{
  /**
   * The process initializes its components including all connections to participating
   * resources.
   */
  INITIALIZATION
  {
    @Override
    public boolean isSystemPhase()
    {
      return true;
    }
  },
  
//  /**
//   * The process runs various kinds of analysis algorithms in order to understand the flow
//   * structure and optimize the processing of the data.
//   */
//  GRAPH_ANALYSIS
//  {
//    @Override
//    public boolean isSystemPhase()
//    {
//      return true;
//    }
//  },
  
  /**
   * Data is flowing.<br>
   * Checkpointing via checkpoint markers works during this phase only!
   */
  COMPUTATION
  {
    @Override
    public boolean isSystemPhase()
    {
      return false;
    }
  },
  
  /**
   * The process tears down all its components and closes all connections to attached resources.
   */
  TEARDOWN
  {
    @Override
    public boolean isSystemPhase()
    {
      return true;
    }
  };
  
  /**
   * Defines whether this phase accepts user input.
   * @return
   */
  abstract public boolean isSystemPhase();
}
