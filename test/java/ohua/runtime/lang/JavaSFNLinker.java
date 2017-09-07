/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.lang.operator.SFNLinker;
import ohua.runtime.engine.utils.FileUtils;
import ohua.lang.defsfn;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The linker part that loads Java SFNs into the registry.
 * @author sertel
 * 
 */
public abstract class JavaSFNLinker {
  /**
   * Loads all operators located inside the Ohua jar used.
   */
  public static void loadCoreOperators() {
    SFNLinker.getInstance();
    load("ohua.lang", true);
  }

  public static Set<String> list() {
    Set<String> linked = new HashSet<>();
    SFNLinker.getInstance().getRegisteredUserOperators();
    linked.addAll(SFNLinker.getInstance().getRegisteredUserOperators());
    return linked;
  }


  public static void registerFunction(String ns, String name, Method handle) {
    SFNLinker opFactory = SFNLinker.getInstance();
    opFactory.registerUserOperator(ns + "/" + name, handle.getDeclaringClass().getName(), true);
  }

  private static void load(final String packageName, boolean reload) {
    String javaPackageName = packageName.replace("-", "_");
    List<Path> paths = FileUtils.loadFromClasspath(javaPackageName.replace(".", "/"), "*.class");
    SFNLinker opFactory = SFNLinker.getInstance();
    for(Path path : paths) {
      String className = path.getFileName().toString().replaceAll(".class$", "");
      try {
        Class<?> cls = Class.forName(javaPackageName + "." + className);
        checkAndRegister(packageName, cls, className, opFactory, reload);
      }
      catch(ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
  }
  
  private static void checkAndRegister(final String packageName, final Class<?> cls, final String className,
                                       SFNLinker opFactory, boolean reload)
  {
    String methodName = null;
    for(Method method : cls.getDeclaredMethods()) {
      if(method.isAnnotationPresent(defsfn.class)) {
        if(methodName == null) {
          methodName = method.getName();
        } else {
          System.err.println("Class '" + cls.getName() + "' declares more than one @Function. Not registered!");
          break;
        }
      }
    }

    if(methodName != null) {
      String javaPackageName = packageName.replace("-", "_");
//      // Java name
//      opFactory.registerUserOperator(methodName, javaPackageName + "." + className, reload);
//      // Clojure name
//      String clojureName = convertToClojureName(methodName);
//      opFactory.registerUserOperator(clojureName, javaPackageName + "." + className, reload);
      // add a namespace-qualified mapping.
      // TODO: this should be the only mapping for this methodName! see issue #101
      opFactory.registerUserOperator(packageName + "/" + methodName, javaPackageName + "." + className, reload);
    }
  }
  
  public static String convertToClojureName(String name) {
    StringBuffer clojureName = new StringBuffer();
    for(int i = 0; i < name.length(); i++) {
      char s = name.charAt(i);
      if(Character.isUpperCase(s)) {
        clojureName.append("-");
        clojureName.append(Character.toLowerCase(s));
      } else {
        clojureName.append(s);
      }
    }
    return clojureName.toString();
  }

  public static void clear() {
    SFNLinker.getInstance().clear();
  }

}
