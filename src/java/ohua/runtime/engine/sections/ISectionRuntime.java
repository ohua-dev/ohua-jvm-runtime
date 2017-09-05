/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.sections;

import java.util.concurrent.Callable;

/**
 * Created by sertel on 1/26/17.
 */
public interface ISectionRuntime<T> extends Callable<T> {
  void startNewSystemPhase();
}
