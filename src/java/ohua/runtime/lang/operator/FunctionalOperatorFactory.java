/*
 * Copyright (c) Sebastian Ertel 2014. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.engine.flowgraph.elements.operator.IOperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

@Deprecated
public class FunctionalOperatorFactory implements IOperatorFactory {
    private static FunctionalOperatorFactory _funcFactory = new FunctionalOperatorFactory();
    private static FunctionalOperatorDeserializer _deserializer = new FunctionalOperatorDeserializer();

    protected FunctionalOperatorFactory() {
    }

    public static FunctionalOperatorFactory getInstance() {
        OperatorFactory fac = OperatorFactory.getInstance();
        fac.setOperatorDescriptorDeserializer(_deserializer);
        return _funcFactory;
    }

    public OperatorCore createUserOperatorCore(FlowGraph graph, String operatorName) throws OperatorLoadingException {
        UserOperator operator = createUserOperatorInstance(operatorName);
        OperatorCore core = OperatorFactory.getInstance().prepareUserOperator(graph, operatorName, operator);
        return core;
    }

    @SuppressWarnings("unchecked")
    public UserOperator createUserOperatorInstance(String operatorName) throws OperatorLoadingException {
        Class<?> clz = OperatorFactory.getInstance().loadOperatorImplementationClass(operatorName);
        if (UserOperator.class.isAssignableFrom(clz)) {
            return OperatorFactory.getInstance().createOperatorInstance((Class<? extends UserOperator>) clz);
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

    @Override
    public boolean exists(String operatorName) {
        return OperatorFactory.getInstance().exists(operatorName);
    }

    public void setApplyDescriptorsForUserOperators(boolean b) {
        OperatorFactory.getInstance().setApplyDescriptorsForUserOperators(b);
    }

    public void registerUserOperator(String alias, String implReference, boolean reload) {
        OperatorFactory.getInstance().registerUserOperator(alias, implReference, reload);
    }

    public void registerUserOperator(String alias, String implReference) {
        OperatorFactory.getInstance().registerUserOperator(alias, implReference);
    }

    public Set<String> getRegisteredUserOperators() {
        return OperatorFactory.getInstance().getRegisteredUserOperators();
    }

    public void clear() {
        OperatorFactory.getInstance().clear();
    }

    public String getSourceCodeReference(String name) {
        return OperatorFactory.getInstance().getSourceCodeReference(name);
    }

    public Collection<String> getRegisteredOperatorClasses() {
        return OperatorFactory.getInstance().getRegisteredOperatorClasses();
    }

}
