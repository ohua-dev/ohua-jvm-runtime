/*
 * Copyright (c) Sebastian Ertel 2008-2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.operators.IOperatorDescriptionProvider;
import ohua.runtime.engine.operators.OperatorDescription;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.lang.operator.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OperatorFactory implements IOperatorFactory
{
  // operators
  private static Map<String, String> _userOperatorRegistry = new HashMap<>();
  private static Map<String, String> _systemOperatorRegistry = new HashMap<>();
  private static Map<String, OperatorDescription> _operatorDescriptors = new HashMap<>();
  private static IOperatorDescriptionProvider _descriptorProvider = new SFNDeserializer();
  private static boolean _applyDescriptorForUserOperators = true;
  private final OperatorID.OperatorIDGenerator _idGen = new OperatorID.OperatorIDGenerator();

  private OperatorFactory() {
  }

  public static OperatorFactory create() {
    return new OperatorFactory();
  }

  public static void setApplyDescriptorsForUserOperators(boolean apply) {
    _applyDescriptorForUserOperators = apply;
  }

  public boolean exists(String operatorName) {
    return _userOperatorRegistry.containsKey(operatorName);
  }

  public UserOperator createUserOperatorInstance(String operatorName) throws OperatorLoadingException {
    Class<?> clz = loadOperatorImplementationClass(operatorName);
    if (UserOperator.class.isAssignableFrom(clz)) {
      return createOperatorInstance((Class<? extends UserOperator>) clz);
    } else {
      Object func = createFunctionObject(clz);
      try {
        Method sf = StatefulFunction.resolveMethod(func);
        AbstractFunctionalOperator op =
                sf.isAnnotationPresent(DataflowFunction.class) ?
                        new FunctionalDataflowOperator() :
                        new FunctionalOperator();
        op.setFunctionObject(func);
        return op;
      } catch (CompilationException ce) {
        throw new OperatorLoadingException(ce);
      }
    }
  }

  private Object createFunctionObject(Class<?> clz) throws OperatorLoadingException {
    return StatefulFunction.createStatefulFunctionObject(clz);
  }

  public OperatorCore createUserOperatorCore(FlowGraph graph,
                                             Class<? extends UserOperator> operatorImplementationClass,
                                             String operatorName) throws OperatorLoadingException {
    UserOperator operator = createOperator(operatorImplementationClass, operatorName);
    OperatorCore core = prepareUserOperator(graph, operatorName, operator);
    return core;
  }

  public OperatorCore createUserOperatorCore(FlowGraph graph, String operatorName) throws OperatorLoadingException {
    UserOperator operator = createUserOperatorInstance(operatorName);
    OperatorCore core = prepareUserOperator(graph, operatorName, operator);
    return core;
  }

  public UserOperator createOperator(FlowGraph graph, String operatorName) throws OperatorLoadingException {
    UserOperator operator = createUserOperatorInstance(operatorName);
    prepareUserOperator(graph, operatorName, operator);
    return operator;
  }

  public OperatorCore
  prepareUserOperator(FlowGraph graph, String operatorName, UserOperator operator)
          throws OperatorLoadingException {
    OperatorCore core = prepareOperator(operatorName, true);
    graph.addOperator(core);
    UserOperatorAdapter adapter = new UserOperatorAdapter(core, operator);
    core.setOperatorAdapter(adapter);
    operator.setOperatorAlgorithmAdapter(adapter);
    return core;
  }

  protected OperatorCore
  prepareSystemOperator(String operatorName, SystemOperator operator) throws OperatorLoadingException {
    OperatorCore core = prepareOperator(operatorName, false);
    SystemOperatorAdapter adapter = new SystemOperatorAdapter(core, operator);
    core.setOperatorAdapter(adapter);
    operator.setOperatorAlgorithmAdapter(adapter);
    return core;
  }

  protected OperatorCore
  prepareOperator(String operatorName, boolean isUserOperator) throws OperatorLoadingException {
    OperatorCore core = new OperatorCore(operatorName, _idGen.generateNewOperatorID());
    if(isUserOperator && !_applyDescriptorForUserOperators) {
      // don't load it here. the application using the ops will define the structure by itself.
    } else {
      OperatorDescription description = loadOperatorDescriptor(operatorName, isUserOperator);
      if(description != null) {
        description.apply(core, isUserOperator);
      } else {
        if(RuntimeProcessConfiguration.LOGGING_ENABLED)
          System.out.println("WARNING: No operator descriptor found for: " + operatorName);
      }
    }
    return core;
  }

  public <T extends UserOperator> T createUserOperator(FlowGraph graph, Class<T> operatorImplementationClass,
                                                       String operatorName) throws OperatorLoadingException {
    T operator = createOperator(operatorImplementationClass, operatorName);
    prepareUserOperator(graph, operatorName, operator);
    return operator;
  }

  private <T extends AbstractOperatorAlgorithm> T
  createOperator(Class<T> operatorImplementationClass, String operatorName) throws OperatorLoadingException {
    T operator = createOperatorInstance(operatorImplementationClass);
    return operator;
  }

  public <T extends AbstractOperatorAlgorithm> T
  createOperatorInstance(Class<T> clz) throws OperatorLoadingException {
    T operator = null;
    try {
      // get the default constructor
      Constructor<T> constructor = clz.getConstructor();
      operator = constructor.newInstance();
    } catch(Exception e) {
      throw new OperatorLoadingException(e);
    }

    return operator;
  }

  public Class<?> loadOperatorImplementationClass(String operatorName) throws OperatorLoadingException {
    if(!_userOperatorRegistry.containsKey(operatorName)) {
      throw new IllegalArgumentException("No registry entry found for operator: " + operatorName);
    }
    String operatorImplementationClass = _userOperatorRegistry.get(operatorName);
    if(operatorImplementationClass == null) {
      throw new OperatorLoadingException("No implementation class found for operator: " + operatorName);
    }
    try {
      return Class.forName(operatorImplementationClass);
    } catch(ClassNotFoundException e) {
      throw new OperatorLoadingException(e);
    }
  }

  /**
   * Use this method to overload specific operator implementations.
   * @param operatorName
   * @param clz
   */
  public void setOperatorImplementationClass(String operatorName, Class<? extends UserOperator> clz) {
    _userOperatorRegistry.put(operatorName, clz.getName());
  }

  private OperatorDescription loadOperatorDescriptor(String operatorName, boolean isUserOperator) throws OperatorLoadingException {
    if(_operatorDescriptors.containsKey(operatorName)) {
      return _operatorDescriptors.get(operatorName);
    }

    String operatorImplName = null;
    if(isUserOperator) {
      if(!_userOperatorRegistry.containsKey(operatorName)) {
        throw new IllegalArgumentException("No registry entry found for operator: " + operatorName);
      } else {
        operatorImplName = _userOperatorRegistry.get(operatorName);
      }
    } else {
      if(!_systemOperatorRegistry.containsKey(operatorName)) {
        return null;
      } else {
        operatorImplName = _systemOperatorRegistry.get(operatorName);
      }
    }

    OperatorDescription opDescriptor = _descriptorProvider.apply(operatorImplName);
    _operatorDescriptors.put(operatorName, opDescriptor);
    return opDescriptor;
  }

  public UserOperator createUserOperator(FlowGraph graph, String operatorType, String displayName) throws OperatorLoadingException {
    UserOperator operator = createUserOperatorInstance(operatorType);
    OperatorCore core = prepareUserOperator(graph, operatorType, operator);
    core.setOperatorName(displayName);
    return operator;
  }

  public OperatorCore createSystemOperatorCore(Class<? extends SystemOperator> clz, String operatorName) {
    OperatorCore core = null;
    try {
      SystemOperator operator = createOperatorInstance(clz);
      core = prepareSystemOperator(operatorName, operator);
    } catch(OperatorLoadingException e) {
      Assertion.impossible(e);
    }
    return core;
  }

  public void setOperatorDescriptorDeserializer(IOperatorDescriptionProvider descriptorProvider) {
    _descriptorProvider = descriptorProvider;
  }

  public static boolean registerUserOperator(String alias, String implReference) {
    return registerUserOperator(alias, implReference, false);
  }

  public static boolean registerUserOperator(String alias, String implReference, boolean update) {
    if(_userOperatorRegistry.containsKey(alias) && !update) {
      return false;
    } else {
      _userOperatorRegistry.put(alias, implReference);
      return true;
    }
  }

  public static boolean registerUserOperator(String alias, Class<? extends UserOperator> opType, OperatorDescription description) {
    _userOperatorRegistry.put(alias, opType.getName());
    _operatorDescriptors.put(alias, description);
    return true;
  }

  public static boolean registerSystemOperator(String alias, Class<? extends SystemOperator> opType, OperatorDescription description) {
    _systemOperatorRegistry.put(alias, opType.getName());
    _operatorDescriptors.put(alias, description);
    return true;
  }

  public static Set<String> getRegisteredUserOperators() {
    return _userOperatorRegistry.keySet();
  }

  public static void clear() {
    _userOperatorRegistry = new HashMap<>();
    _systemOperatorRegistry = new HashMap<>();
  }
}
