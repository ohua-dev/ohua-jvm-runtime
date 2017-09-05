/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine;

/**
 * The interface to be implemented in order to receive system events.
 * @author sertel
 * 
 */
public interface ISystemEventListener
{
  public void notifyOnEvent(EngineEvents event);
}
