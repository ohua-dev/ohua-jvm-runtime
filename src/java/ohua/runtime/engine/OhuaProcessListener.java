/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

/**
 * A callback interface for all components interested in process events that got triggered in
 * response to user events such as START_COMPUTATION, FINISH_COMPUTATION and TEARDOWN.
 * @author sertel
 * 
 */
public interface OhuaProcessListener
{
  public void completed(UserRequest request, Throwable t);
}
