/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
// Make this annotation accessible at runtime via
// reflection.
@Target({ ElementType.METHOD })
// This annotation can only be applied to class methods.
public @interface defsfn {
  /**
   * Java semantics: When only an object array is passed to the var args slot then this array becomes the var-args array.
   * This switch allows to disable this semantics. Then the single object array becomes just the first entry in the var-args
   * array.
   */
  boolean useJavaVarArgsSemantics() default true;
}
