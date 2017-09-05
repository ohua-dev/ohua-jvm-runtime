/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

/**
 * Implement this class as a closure to avoid dangling references!
 * 
 * @author sertel
 *
 * @param <T>
 */
public interface WriteReference<T>
{
  public void set(T value);
}
