/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.test.util;

import java.lang.reflect.Field;

/**
 * Created by sertel on 1/30/17.
 */
public abstract class TestUtil {

  public static <T> T getPrivateFieldReference(String name, Object fieldOwner) {
    return getPrivateFieldReference(fieldOwner.getClass(), name, fieldOwner);
  }

  @SuppressWarnings("unchecked")
  public static <T> T getPrivateFieldReference(Class<?> ownerClz, String name, Object fieldOwner) {
    try {
      Field f = ownerClz.getDeclaredField(name);
      f.setAccessible(true); // allow for reflection access
      return (T) f.get(fieldOwner);
    }
    catch(Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
