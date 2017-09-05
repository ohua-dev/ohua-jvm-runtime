/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

import java.io.Serializable;

public enum ProcessState implements Serializable
{
  /**
   * The process is waiting to be started again by the a new user request.
   */
  IDLE,
  
  /**
   * The process is streaming data.
   */
  RUNNING,
  
  /**
   * The process was shut down and no further user requests will be processed.
   */
  DONE
}
