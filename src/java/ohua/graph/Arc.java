/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.graph;


public final class Arc {
  public final Source source;
  public final Target target;

  public Arc(final Source source, final Target target) {
    this.source = source;
    this.target = target;
  }
}