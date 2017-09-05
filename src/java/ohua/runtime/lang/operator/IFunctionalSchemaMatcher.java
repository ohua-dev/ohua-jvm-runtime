/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

/**
 * Created by sertel on 3/24/16.
 */
public interface IFunctionalSchemaMatcher {

  default Object[] matchInputSchema(AbstractSchemaMatcher matcher) {
    while (true) {
      if (!matcher.isCallDataAvailable()) return null;

      if (matcher.isExecuteCall()) break;
      else continue;
    }
    return matcher.loadCallData();
  }

}
