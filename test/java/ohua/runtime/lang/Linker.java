/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.lang.operator.FunctionalOperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorLibrary;
import ohua.runtime.engine.utils.FileUtils;
import ohua.lang.defsfn;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.exceptions.CompilationException.CAUSE;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The linker part of the compiler. It is directly called from the Clojure macro and loads all
 * referenced operators into the operator registry.
 * @author sertel
 * 
 */
@Deprecated
public abstract class Linker {
  /**
   * Loads all operators located inside the Ohua jar used.
   */
  public static void loadCoreOperators() {
    FunctionalOperatorFactory.getInstance();
  }
  
  /**
   * Loads all additional registries and adds the loaded operators to the library.
   * @param libs
   * @throws Exception
   * @throws FileNotFoundException
   */
  public static void loadAppOperators(String... libs) throws Exception {
    for(String opLib : libs) {
      Map<String, String> ops = OperatorLibrary.loadLibrary(opLib);
      for(Map.Entry<String, String> op : ops.entrySet()) {
        FunctionalOperatorFactory.getInstance().registerUserOperator(op.getKey(), op.getValue());
      }
    }
  }
  
  public static Set<String> list() {
    Set<String> linked = new HashSet<>();
    FunctionalOperatorFactory.getInstance().getRegisteredUserOperators();
    linked.addAll(FunctionalOperatorFactory.getInstance().getRegisteredUserOperators());
    return linked;
  }
  
  /**
   * Loads all additional operators/functions from a package and adds them to the library.
   * @param packageName
   * @throws Exception
   * @throws FileNotFoundException
   */

  public static void reload(final String packageName) {
    load(packageName, true);
  }
  
  public static Object resolve(final String sfRef) throws Exception {
//    if(isOperator(sfRef)) {
//      return loadFunction(sfRef);
//    } else {
//      String convertedRef = convertFunctionName(sfRef);
//      if(isOperator(convertedRef)) {
//        return loadFunction(convertedRef);
//      } else {
        throw new CompilationException(CAUSE.NO_METHOD_FOUND, sfRef);
//      }
//    }
  }

//  private static Object loadFunction(final String sfRef) throws Exception {
//    String sourceRef = FunctionalOperatorFactory.getInstance().getSourceCodeReference(sfRef);
//    return ReflectionUtils.createDefault(Class.forName(sourceRef));
//  }

  public static String convertFunctionName(String functionRef) {
    StringBuffer converted = new StringBuffer(functionRef);
    int idx = -1;
    while((idx = converted.indexOf("-")) > -1) {
      converted.deleteCharAt(idx);
      converted.replace(idx, idx + 1, String.valueOf(converted.charAt(idx)).toUpperCase());
    }
    return converted.toString();
  }
  
  private static void load(final String packageName, boolean reload) {
    String javaPackageName = packageName.replace("-", "_");
    List<Path> paths = FileUtils.loadFromClasspath(javaPackageName.replace(".", "/"), "*.class");
//    OperatorFactory.registryFilter = "{Ohua,Language}*Registry.xml";
    FunctionalOperatorFactory opFactory = FunctionalOperatorFactory.getInstance();
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
                                       FunctionalOperatorFactory opFactory, boolean reload)
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
      // Java name
      opFactory.registerUserOperator(methodName, javaPackageName + "." + className, reload);
      // Clojure name
      String clojureName = convertToClojureName(methodName);
      opFactory.registerUserOperator(clojureName, javaPackageName + "." + className, reload);
      // add a namespace-qualified mapping.
      // TODO: this should be the only mapping for this methodName! see issue #101
      opFactory.registerUserOperator(packageName + "/" + clojureName, javaPackageName + "." + className, reload);
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
    FunctionalOperatorFactory.getInstance().clear();
  }

}
