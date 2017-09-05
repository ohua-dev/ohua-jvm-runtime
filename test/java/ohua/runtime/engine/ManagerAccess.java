/*
 * Copyright (c) Sebastian Ertel 2012. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.RuntimeProcessConfiguration;

public abstract class ManagerAccess
{
  public static RuntimeProcessConfiguration getRuntimeConfig(AbstractProcessManager manager)
  {
    return manager.getRuntimeProcessConfiguration();
  }
}
