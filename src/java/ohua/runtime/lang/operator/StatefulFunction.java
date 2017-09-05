/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.exceptions.OperatorLoadingException;
import ohua.runtime.engine.utils.ReflectionUtils;
import ohua.lang.Apply;
import ohua.lang.defsfn;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.exceptions.CompilationException.CAUSE;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class StatefulFunction {

    private Method _m = null;
    private MethodHandle _mh = null;

    private StatefulFunction(Method m, MethodHandle mh) {
        _m = m;
        _mh = mh;
    }

    protected static OutputMatch.OutputMatcher<?> getOutputMatcher(Class<?> functionType, Class<?> returnType, OutputMatch.MATCH_TYPE match) {
        if (Apply.class.isAssignableFrom(functionType)) {
      /* apply is a bit more expensive because we can not define a matcher at compile-time. */
            return new OutputMatch.OutputMatcher<Object>() {
                @Override
                public Object matchTyped(OutputPortControl outPort, int[] indexes, Object result) {
                    return OutputMatch.selectMatch(result.getClass(), match).match(outPort, indexes, result);
                }

                @Override
                public Object allInTyped(Object result) {
                    return OutputMatch.selectMatch(result.getClass(), match).allIn(result);
                }
            };
        } else {
            return OutputMatch.selectMatch(returnType, match);
        }
    }

    public static Method resolveMethod(Object functionObject) throws CompilationException {
        List<Method> candidates = new ArrayList<>();
        for (Method m : functionObject.getClass().getMethods()) {
            if (Arrays.binarySearch(AbstractFunctionalOperator.class.getMethods(), m, new Comparator<Method>() {
                @Override
                public int compare(Method o1, Method o2) {
                    return o1 == o2 ? 0 : 1;
                }
            }) < 0) {
                if (m.isAnnotationPresent(defsfn.class)) {
                    candidates.add(m);
                }
            }
        }
        if (candidates.isEmpty()) throw new CompilationException(CAUSE.NO_METHOD_FOUND);
        else if (candidates.size() == 1) return candidates.get(0);
        else {
            List<Method> candidatesInThis = new ArrayList<>();
            for (Method candidate : candidates) {
                if (candidate.getDeclaringClass().equals(functionObject.getClass())) candidatesInThis.add(candidate);
            }
            if (candidatesInThis.size() == 1) return candidatesInThis.get(0);
            else throw new CompilationException(CAUSE.METHOD_AMBIGUITY, functionObject.getClass().getName());
        }
    }

    public static StatefulFunction resolve(Object functionObject) throws CompilationException {
        Method m = resolveMethod(functionObject);
        return new StatefulFunction(m, prepareHandle(functionObject, m));
    }

    private static MethodHandle prepareHandle(Object functionObject, Method m) throws IllegalArgumentException,
            CompilationException {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle methodHandle = null;
        try {
            methodHandle = lookup.unreflect(m);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assertion.impossible();
        }
        methodHandle = methodHandle.bindTo(functionObject);
        methodHandle = methodHandle.asSpreader(Object[].class, m.getParameterCount());
        CallSite callSiteMethod = new ConstantCallSite(methodHandle);
        methodHandle = callSiteMethod.dynamicInvoker();
        return methodHandle;
    }

    public static Object invoke(Method m, Object obj, Object[] args) throws IllegalArgumentException,
            InvocationTargetException,
            IllegalAccessException {
        return m.invoke(obj, args);
    }

    public static Object createStatefulFunctionObject(Class<?> clz) throws OperatorLoadingException {
        try {
            return ReflectionUtils.createDefault(clz);
        } catch (Exception e) {
            throw new OperatorLoadingException(e);
        }
    }

    public Method getMethod() {
        return _m;
    }

    public Object invoke(Object[] args) throws Throwable {
        // does not work:
        // http://stackoverflow.com/questions/16005824/some-basic-questions-about-methodhandle-api
        // return (Object[]) _mh.invoke(args);

        // adds quite a substantial performance penalty
        // return (Object[]) _mh.invokeWithArguments(args);

        // -> spreader needed! see prepare()
//        System.out.println(Arrays.deepToString(args));
        return _mh.invoke(args);

    }
}
