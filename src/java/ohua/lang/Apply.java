/*
 * ohua : Apply.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import ohua.runtime.lang.operator.StatefulFunction;

import java.util.function.BiFunction;

public class Apply {

  private final IFn apply_ = Clojure.var("clojure.core/apply");
  private BiFunction<Object, Object[], Object> fn;


  public Apply() {

    fn = (function, args) -> {


      if (function instanceof IFn)
        fn = apply_::invoke;
      else if(function instanceof Partial.PartialFunction)
        fn = (_0, a) -> {
          try {
            return ((Partial.PartialFunction)function).apply(args);
          }catch(Throwable t){
            throw new RuntimeException(t);
          }
        };
      else if (function instanceof StatefulFunction)
        fn = (_0, args_) -> {
          try {
            return ((StatefulFunction) function).invoke(args_);
          } catch (Throwable throwable) {
            throwable.printStackTrace();
            return null;
          }
        };
//      else if (function instanceof Class) {
//
//        try {
//          Object o = StatefulFunction.createStatefulFunctionObject((Class) function);
//          StatefulFunction sf = StatefulFunction.resolve(o);
//          fn = (_0, args_) -> {
//            try {
//              return sf.invoke(args_);
//            } catch (Throwable throwable) {
//              throwable.printStackTrace();
//              return null;
//            }
//          };
//        } catch (Throwable throwable) {
//          throwable.printStackTrace();
//          return null;
//        }
//      }
      else {
        throw new RuntimeException("Unexpected type as function argument: " + function.getClass().toString());
      }

      return fn.apply(function, args);
    };

  }
  
  @defsfn
  public Object apply(Object function, Object... args) throws Throwable {
    return fn.apply(function, args);
  }
}
