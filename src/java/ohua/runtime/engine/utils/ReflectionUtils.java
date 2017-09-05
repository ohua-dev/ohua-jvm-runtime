/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectionUtils
{
  public static void setProperty(Object target, String propertyName, Object value)
  {
    boolean success = setProperty(target, propertyName, value, value.getClass());
    if(success)
    {
      return;
    }
    
    success = setPropertyViaSuperClass(target, propertyName, value, value.getClass());
    if(success)
    {
      return;
    }
    
    setPropertyViaInterface(target, propertyName, value, value.getClass());
  }
  
  private static boolean setProperty(Object target,
                                     String propertyName,
                                     Object value,
                                     Class<?> valueClass)
  {
    return setProperty(target, propertyName, value, valueClass, target.getClass());
  }
  
  private static boolean setProperty(Object target,
                                     String propertyName,
                                     Object value,
                                     Class<?> valueClass,
                                     Class<?> targetClass)
  {
    if(!setPropertyViaDirectMemberAccess(target, propertyName, value, targetClass))
    {
      boolean success = false;
      Class<?> currentClass = valueClass;
      
      while(!currentClass.getCanonicalName().equals("java.lang.Object") && !success)
      {
        success =
            setPropertyViaMethodCall(target, propertyName, value, currentClass, targetClass);
        currentClass = currentClass.getSuperclass();
      }
      
      return success;
    }
    else
    {
      return true;
    }
  }
  
  private static boolean setPropertyViaDirectMemberAccess(Object target,
                                                          String propertyName,
                                                          Object value,
                                                          Class<?> targetClass)
  {
    if(!setPropertyDirectly(target, propertyName, value, targetClass))
    {
      return setPropertyDirectly(target, "_" + propertyName, value, targetClass);
    }
    return true;
  }
  
  private static boolean setPropertyDirectly(Object target,
                                             String propertyName,
                                             Object value,
                                             Class<?> targetClass)
  {
    try
    {
      Field member = targetClass.getDeclaredField(propertyName);
      member.set(target, value);
    }
    catch(SecurityException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(NoSuchFieldException e)
    {
      return false;
    }
    catch(IllegalArgumentException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(IllegalAccessException e)
    {
      return false;
    }
    return true;
  }
  
  private static boolean setPropertyViaMethodCall(Object target,
                                                  String propertyName,
                                                  Object value,
                                                  Class<?> valueClass,
                                                  Class<?> targetClass)
  {
    String setter =
        "set" + propertyName.substring(0, 1).toUpperCase()
            + propertyName.substring(1, propertyName.length());
    try
    {
      Method m = targetClass.getMethod(setter, valueClass);
      m.invoke(target, value);
    }
    catch(SecurityException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(NoSuchMethodException e)
    {
      return false;
    }
    catch(IllegalArgumentException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(IllegalAccessException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(InvocationTargetException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return true;
  }
  
  private static boolean setPropertyViaSuperClass(Object target,
                                                  String propertyName,
                                                  Object value,
                                                  Class<?> valueClass)
  {
    boolean success = false;
    Class<?> currentClass = target.getClass().getSuperclass();
    
    while(!currentClass.getCanonicalName().equals("java.lang.Object") && !success)
    {
      success = setProperty(target, propertyName, value, valueClass, currentClass);
      currentClass = currentClass.getSuperclass();
    }
    
    return success;
  }
  
  private static void setPropertyViaInterface(Object target,
                                              String propertyName,
                                              Object value,
                                              Class<? extends Object> class1)
  {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  public static Object getPropertyViaDirectMemberAccess(Object target,
                                                        String propertyName,
                                                        Class<?> targetClass)
  {
    Object value = getPropertyDirectly(target, propertyName, targetClass);
    if(value == null)
    {
      value = getPropertyDirectly(target, "_" + propertyName, targetClass);
    }
    return value;
  }
  
  public static Object getPropertyDirectly(Object target,
                                           String propertyName,
                                           Class<?> targetClass)
  {
    try
    {
      Field member = targetClass.getDeclaredField(propertyName);
      return member.get(target);
    }
    catch(SecurityException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(NoSuchFieldException e)
    {
      return null;
    }
    catch(IllegalArgumentException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(IllegalAccessException e)
    {
      return null;
    }
  }
  
  public static Object getPropertyViaMethodCall(Object target,
                                                String propertyName,
                                                Class<?> targetClass)
  {
    String getter =
        "get" + propertyName.substring(0, 1).toUpperCase()
            + propertyName.substring(1, propertyName.length());
    try
    {
      Method m = targetClass.getMethod(getter);
      return m.invoke(target);
    }
    catch(SecurityException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(NoSuchMethodException e)
    {
      return null;
    }
    catch(IllegalArgumentException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(IllegalAccessException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    catch(InvocationTargetException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  public static Object getProperty(Object target, String propertyName, Class<?> targetClass)
  {
    Object prop = getPropertyViaDirectMemberAccess(target, propertyName, targetClass);
    if(prop == null)
    {
      prop = getPropertyViaMethodCall(target, propertyName, targetClass);
    }
    return prop;
  }
  
  public static <T> T createDefault(Class<T> clz) throws InstantiationException, IllegalAccessException,
                                                 IllegalArgumentException, InvocationTargetException,
                                                 NoSuchMethodException, SecurityException{
    // get the default constructor
    Constructor<T> constructor = clz.getConstructor();
    return constructor.newInstance();
  }
}
