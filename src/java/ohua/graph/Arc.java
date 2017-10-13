/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public final class Arc<T> {
  public final Source<T> source;
  public final Target target;

  public Arc(final Target target, final Source<T> source) {
    this.source = source;
    this.target = target;
  }
}
