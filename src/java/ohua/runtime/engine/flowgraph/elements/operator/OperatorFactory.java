/*
 * Copyright (c) Sebastian Ertel 2008-2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.exceptions.XMLParserException;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.utils.parser.OperatorDescription;
import ohua.runtime.engine.utils.parser.OperatorDescriptorDeserializer;
import ohua.runtime.engine.utils.parser.OperatorMappingParser;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Deprecated // TODO just remove the XML stuff here! descriptors can be inner classes of the ops.
public class OperatorFactory implements IOperatorFactory
{
  // operators
  private static Map<String, String> _userOperatorRegistry = new HashMap<>();
  private static Map<String, String> _systemOperatorRegistry = new HashMap<>();
  private static OperatorFactory _factory = new OperatorFactory();
  private Map<String, OperatorDescription> _operatorDescriptors = new HashMap<>();
  private OperatorDescriptorDeserializer _descriptorDeserializer = new OperatorDescriptorDeserializer();
  private boolean _applyDescriptorForUserOperators = true;

  private OperatorFactory() {
    // singleton
  }

  public static OperatorFactory getInstance() {
//    if(_userOperatorRegistry == null) {
//      OperatorMappingParser parser = new OperatorMappingParser();
//      try {
//        // TODO this should probably not use the system operators because on listing all user
//        // ops there should be no system ops listed!
//        _userOperatorRegistry = parser.loadOperatorMappings(registryFilter);
//      } catch(XMLParserException e) {
//        throw new RuntimeException(e);
//      }
//    }
//
//    if(_systemOperatorRegistry == null) {
//      OperatorMappingParser parser = new OperatorMappingParser();
//      try {
//        _systemOperatorRegistry = parser.loadOperatorMappings("*SystemComponentRegistry.xml");
//      } catch(XMLParserException e) {
//        throw new RuntimeException(e);
//      }
//    }

    return _factory;
  }

  public static OperatorDescription getOperatorDescription(OperatorCore operator) {
    return getInstance().getOperatorDescription(operator.getOperatorType());
  }

  public void setApplyDescriptorsForUserOperators(boolean apply) {
    _applyDescriptorForUserOperators = apply;
  }

  public boolean exists(String operatorName) {
    return _userOperatorRegistry.containsKey(operatorName);
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
    OperatorCore core = new OperatorCore(operatorName);
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

  @SuppressWarnings("unchecked") public UserOperator
  createUserOperatorInstance(String operatorName) throws OperatorLoadingException {
    Class<? extends UserOperator> clz =
            (Class<? extends UserOperator>) loadOperatorImplementationClass(operatorName);
    return createOperatorInstance(clz);
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
    Class<?> clz = null;
    try {
      clz = Class.forName(operatorImplementationClass);
    } catch(ClassNotFoundException e) {
      throw new OperatorLoadingException(e);
    }
    return clz;
  }

  /**
   * Use this method to overload specific operator implementations.
   * @param operatorName
   * @param clz
   */
  public void setOperatorImplementationClass(String operatorName, Class<? extends UserOperator> clz) {
    _userOperatorRegistry.put(operatorName, clz.getName());
  }

  // FIXME redo this!
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

    OperatorDescription opDescriptor = _descriptorDeserializer.deserialize(operatorImplName);
    _operatorDescriptors.put(operatorName, opDescriptor);
    return opDescriptor;
  }

  public OperatorDescription getOperatorDescription(String operatorName) {
    return _operatorDescriptors.get(operatorName);
  }

  public String getSourceCodeReference(String operatorName) {
    return _userOperatorRegistry.get(operatorName);
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

  public void setOperatorDescriptorDeserializer(OperatorDescriptorDeserializer descriptorDeserializer) {
    _descriptorDeserializer = descriptorDeserializer;
  }

  public boolean registerUserOperator(String alias, String implReference) {
    return registerUserOperator(alias, implReference, false);
  }

  public boolean registerUserOperator(String alias, String implReference, boolean update) {
    if(_userOperatorRegistry.containsKey(alias) && !update) {
      return false;
    } else {
      _userOperatorRegistry.put(alias, implReference);
      return true;
    }
  }

  public boolean registerUserOperator(String alias, Class<? extends UserOperator> opType, OperatorDescription description) {
    _userOperatorRegistry.put(alias, opType.getName());
    _operatorDescriptors.put(alias, description);
    return true;
  }

  public boolean registerSystemOperator(String alias, Class<? extends SystemOperator> opType, OperatorDescription description) {
    _systemOperatorRegistry.put(alias, opType.getName());
    _operatorDescriptors.put(alias, description);
    return true;
  }

  public Set<String> getRegisteredUserOperators() {
    return _userOperatorRegistry.keySet();
  }

  public Collection<String> getRegisteredOperatorClasses() {
    return _userOperatorRegistry.values();
  }

  public void clear() {
    _userOperatorRegistry = null;
    _systemOperatorRegistry = null;
  }
}
