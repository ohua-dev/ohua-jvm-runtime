/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.util.Util;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.*;
import org.codehaus.janino.util.ClassFile;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class CallArrayConstruction {

  protected Object[] _currentArray = null;
  private Object[] _callArray = null;
  private Object[] _compoundArray = null;
  private Load[] _loadFns = null;

  void prepare(List<AbstractSchemaMatcher.ActualToFormal> actualToFormals,
               List<AbstractSchemaMatcher.EnvArgToFormal> envArgToFormals,
               Class<?>[] formals,
               boolean isVarArgs, boolean useJavaVarArgsSemantics) {

    /**
     * _callArray = new Object[actualToFormals.size() + envArgToFormals.size()];</br>
     * This is not sufficient because it is possible in the case of var args to no assign anything to the var args slot.
     * In this case the value of the actual is null.</br>
     * Hence, the size of this array is always defined via the size of the formals defined.
     */
    _callArray = new Object[formals.length];

    // create load functions for all slots
    int numLoadFns = actualToFormals.size() + envArgToFormals.size();
    _loadFns = new Load[numLoadFns];
    for (int i = 0; i < numLoadFns; i++) {
      final int formalSlotIdx = i;
      if (envArgToFormals.stream().anyMatch(e -> e._formalSlotIdx == formalSlotIdx)) {
        List<AbstractSchemaMatcher.EnvArgToFormal> envArgToFormal = envArgToFormals.stream().filter(e -> e._formalSlotIdx == formalSlotIdx).collect(Collectors.toList());
        Assertion.invariant(envArgToFormal.size() == 1, () -> "Duplicate (env arg) mapping for formal slot detected: " + envArgToFormal.stream().map(e -> e._formalSlotIdx));
        Supplier<Object> envArgRetrieval = createEnvArgRetrievalFunction(envArgToFormal.get(0));
        _loadFns[i] = new EnvArgsLoader(envArgRetrieval);
      } else {
        Assertion.invariant(actualToFormals.stream().anyMatch(a -> a._formalSlotIdx == formalSlotIdx), () -> "Unmapped formal slot detected: " + formalSlotIdx);
        List<AbstractSchemaMatcher.ActualToFormal> actualToFormal = actualToFormals.stream().filter(a -> a._formalSlotIdx == formalSlotIdx).collect(Collectors.toList());
        Assertion.invariant(actualToFormal.size() == 1, () -> "Duplicate (actuals) mapping for formal slot detected: " + actualToFormal.stream().map(e -> e._formalSlotIdx));
        // create the closure
        Supplier<Object> dataRetrieval = createValueRetrievalFunction(actualToFormal.get(0));
        _loadFns[i] = new DataLoader(dataRetrieval);
      }
    }

    if (isVarArgs && numLoadFns > 0) {
      _prepareCompound(formals, useJavaVarArgsSemantics, actualToFormals.size() + envArgToFormals.size());
    }
    _currentArray = _callArray;
  }

  public class EnvArgsLoader implements Load {
    private Supplier<Object> envArgRetrieval = null;
    private EnvArgsLoader(Supplier<Object> envArgRetrieval) {this.envArgRetrieval = envArgRetrieval;}

    @Override
    public int load(int currentIdx) {
      _currentArray[currentIdx] = envArgRetrieval.get();
      return ++currentIdx;
    }

    @Override
    public int size() {
      return 1;
    }
  }

  public class DataLoader implements Load {
    private Supplier<Object> dataRetrieval = null;
    private DataLoader(Supplier<Object> dataRetrieval) {this.dataRetrieval = dataRetrieval;}

    @Override
    public int load(int currentIdx) {
      // explicit match
      _currentArray[currentIdx] = dataRetrieval.get();
      return currentIdx + 1;
    }

    @Override
    public int size() {
      return 1;
    }
  }

  protected Supplier<Object> createEnvArgRetrievalFunction(AbstractSchemaMatcher.EnvArgToFormal e) {
    return () -> e._envArg;
  }

  protected Supplier<Object> createValueRetrievalFunction(AbstractSchemaMatcher.ActualToFormal a) {
    switch (a._matchType) {
      case EXPLICIT:
        return () -> ((Object[]) a._inControl.getData())[a._actualDataIdx];
      case IMPLICIT:
        return a._inControl::getData;
    }

    throw new RuntimeException("unknown match type encountered.");
  }

  Object[] construct() {
    int currentIdx = 0;
    for (int i = 0;i < _loadFns.length;i++) {
      Load l = _loadFns[i];
      currentIdx = l.load(currentIdx);
    }
    return _callArray;
  }

  private void _prepareCompound(final Class<?>[] formals, boolean useJavaVarArgsSemantics, int numActuals){
    int lastFormalSlot = formals.length - 1;
    // this call to getComponentType is super expensive! so do it only once and store the result in the closure.
    Class<?> arrayType = formals[lastFormalSlot].getComponentType();

    if(formals.length > numActuals){
      _callArray[lastFormalSlot] = null;
    }else {
      _loadFns[lastFormalSlot] = new CompoundArgsLoader(lastFormalSlot, arrayType, numActuals - lastFormalSlot, _loadFns[lastFormalSlot]);
      _loadFns[numActuals - 1] = new VarArgsLoader(lastFormalSlot, useJavaVarArgsSemantics, _loadFns[numActuals - 1]);
    }
  }

  private final static AtomicLong COMPILED_CLASS_INDEX = new AtomicLong();

  private final static class JaninoRestrictedClassLoader extends
          SecureClassLoader {
    Class<?> defineClass(String name, byte[] b) {
      return defineClass(name, b, 0, b.length, new ProtectionDomain(null,
              new Permissions(), this, null));
    }
  }

  public class CompoundArgsLoader implements Load {
    private int lastFormalSlot = -1;
    private Supplier<Object[]> cloneArray = null;
    private Load firstCompoundArgLoader = null;

    private CompoundArgsLoader(int lastFormalSlot, Class<?> arrayType, int numVarArgs, Load firstCompoundArgLoader){
      this.lastFormalSlot = lastFormalSlot;
      try{
        this.cloneArray = prepareJaninoExpression(arrayType, numVarArgs);
      }catch(Exception e){
        this.cloneArray = arrayType == Object.class ? () -> new Object[numVarArgs] :
                // creating an array like this is also super expensive!
                () -> (Object[]) Array.newInstance(arrayType, numVarArgs);
      }
      this.firstCompoundArgLoader = firstCompoundArgLoader;
    }

    private Util.ThrowingSupplier<Object[]> prepareJaninoExpression(Class<?> arrayType, int length) throws CompileException {
      try {
        // fast expression
        String classPackage = getClass().getPackage().getName() + ".compiled";
        String className = "JaninoCompiledFastexpr"
                + COMPILED_CLASS_INDEX.incrementAndGet();
        String source = "package " + classPackage + ";\n"
                + "public final class "
                + className + " implements "
                + Util.ThrowingSupplier.class.getCanonicalName() + " {\n"
                + "public Object[] get() {\n"
                + "return " + "new " + arrayType.getCanonicalName() + "[" + length + "];\n" + "}\n" + "}";
        Scanner scanner = new Scanner(null, new ByteArrayInputStream(
                source.getBytes("UTF-8")), "UTF-8");

        JaninoRestrictedClassLoader cl = new JaninoRestrictedClassLoader();
        UnitCompiler unitCompiler = new UnitCompiler(
                new Parser(scanner).parseCompilationUnit(),
                new ClassLoaderIClassLoader(cl));
        ClassFile[] classFiles = unitCompiler.compileUnit(true, true, true);
        Class<?> clazz = cl.defineClass(classPackage + "." + className,
                classFiles[0].toByteArray());
        return (Util.ThrowingSupplier<Object[]>) clazz.newInstance();
      }catch(Exception e){
        throw new CompileException(e.getMessage(), null, e);
      }

      // slow expression
//      ExpressionEvaluator ee = new ExpressionEvaluator();
//      ee.setExpressionType(arrayType);
//      ee.cook("new " + arrayType.getCanonicalName() + "[" + length + "]");
//      Object[] args = new Object[0];
//      return () -> (Object[]) ee.evaluate(args);
    }

    @Override
    public int load(int currentIdx) {
      /*
       * note: we can not reuse the compound array because it is essentially a
       * parameter to the function and therewith owned by the function. there are
       * some functions that just pass arguments straight through. in that case,
       * we would essentially share the compound array across calls and overwrite
       * what was previously emitted!
       */
      // create the compound array (resizing is enabled in the wrappers of the
      // following load functions)
      _compoundArray = cloneArray.get();
      // store the compound array at the according place in the call array
      _callArray[lastFormalSlot] = _compoundArray;
      _currentArray = _compoundArray; // switch
      return firstCompoundArgLoader.load(0); // reset to match the current index in the compound array
    }
  }

  public class VarArgsLoader implements Load {
    private int lastFormalSlot = -1;
    private boolean useJavaVarArgsSemantics = false;
    private Load lastCompoundArgLoader = null;

    private Runnable _javaVarArgs = () -> {
      if (useJavaVarArgsSemantics && _currentArray.length == 1 && _currentArray[0] != null &&
              _currentArray[0].getClass().isArray()) {
        // this is the default java way of handling the case when a single array is passed to the position of the var args array:
        // it becomes the var args array itself.
        Runnable javaVarArgs = () -> _callArray[lastFormalSlot] = _currentArray[0];
        javaVarArgs.run(); // execute it for the current call
        _javaVarArgs = javaVarArgs;
      } else {
        _javaVarArgs = () -> {};
      }
    };

    private VarArgsLoader(int lastFormalSlot, boolean useJavaVarArgsSemantics, Load lastCompoundArgLoader){
      this.lastFormalSlot = lastFormalSlot;
      this.useJavaVarArgsSemantics = useJavaVarArgsSemantics;
      this.lastCompoundArgLoader = lastCompoundArgLoader;
    }

    @Override
    public int load(int currentIdx) {
      int newIdx = lastCompoundArgLoader.load(currentIdx);
      _javaVarArgs.run(); // support Java var args
      _currentArray = _callArray; // just do the switch. no handling of env args
      return newIdx;
    }
  }

  // TODO refactor to take lambda functions
  interface Load {
    int load(int currentIdx);

    default int size() {
      return 0;
    }
  }

}
