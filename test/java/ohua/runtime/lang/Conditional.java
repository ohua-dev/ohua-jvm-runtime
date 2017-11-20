/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

/**
 * Created by sertel on 6/24/16.
 */
public class Conditional {
  @defsfn
  public Object[] ifThenElse(Condition cond, Object... conditionArgs) {
    boolean res = cond.check(conditionArgs);
    Object[] result = { res,
            !res };

    return result;
  }
}
