/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by sebastianertel on 3/23/16.
 */
@Retention(RetentionPolicy.RUNTIME)
// Make this annotation accessible at runtime via reflection.
@Target({ElementType.METHOD})
// This annotation can only be applied to class methods.
public @interface DataflowFunction {
  enum Finish {
    DONE
  }

  enum Control {
    DROP
  }
}
