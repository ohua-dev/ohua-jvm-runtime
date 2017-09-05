/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import ohua.runtime.lang.operator.DataflowFunction;

/**
 * Derive from this class in order to provide more knowledge about your return types to Ohua.
 *
 * Created by sertel on 9/20/16.
 */
public abstract class Either<EITHER, OR, MYSELF> {
    public Object _data = null;

    public final MYSELF add(Object data) {
      _data = data;
      return (MYSELF) this;
    }

  public static class EitherObjectArrayOrFinish extends Either<Object[], DataflowFunction.Finish, EitherObjectArrayOrFinish>{}
  public static class EitherObjectOrFinish extends Either<Object, DataflowFunction.Finish, EitherObjectOrFinish>{}
}
