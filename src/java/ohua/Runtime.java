/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua;

import ohua.graph.Graph;
import ohua.runtime.lang.OhuaRuntime;

/**
 * Created by sertel on 9/7/17.
 */
public abstract class Runtime {

  public static Runnable prepare(Graph graph) {
    OhuaRuntime runtime = new OhuaRuntime();

    // TODO take the graph and turn it into calls to this backend

    return () -> {
      try {
        runtime.execute();
      } catch (Throwable e) {
        assert e instanceof RuntimeException;
        throw (RuntimeException) e;
      }
    };
  }

}
