/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.OutputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.lang.Continuations;
import ohua.lang.Either;
import ohua.lang.NonStrict;
import ohua.runtime.exceptions.CompilationException;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by sertel on 3/24/16.
 */
public abstract class DataflowSchemaMatching {

  public static abstract class AbstractDataflowSchemaMatcher extends AbstractSchemaMatcher implements IFunctionalSchemaMatcher {

    // state
    private Object[][] _pending = null;
    private ContinuationRuntime.ContinuationsAwareDataRetrieval _contsDataRetrieval = null;
    private Function<Object, Boolean> _output = this::matchOutput;

    public AbstractDataflowSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    public void compile(List<String> inputs, List<String> outputs) throws CompilationException {
      if (Arrays.stream(_funcOp.getFormalArguments()).anyMatch(this::isNonStrict)) {
        if (!Continuations.class.isAssignableFrom(_funcOp.getReturnType())) {
          throw new CompilationException(CompilationException.CAUSE.WRONG_RETURN_TYPE);
        } else {
          // all good
        }
      } else {
        // no NonStricts there.
      }
      super.compile(inputs, outputs);
    }

    private boolean isNonStrict(Class<?> c) {
      return NonStrict.class.isAssignableFrom(c) || c.isArray() && NonStrict.class.isAssignableFrom(c.getComponentType());
    }

    protected void prepareInputSide(List<ActualToFormal> actualsToFormals, List<EnvArgToFormal> envArgsToFormals) {
      if (Arrays.stream(_funcOp.getFormalArguments()).anyMatch(this::isNonStrict)) {
        List<ActualToFormal> nonStrictActualsToFormals = actualsToFormals.stream().filter(a -> NonStrict.class.isAssignableFrom(a._formalType)).collect(Collectors.toList());
        List<ActualToFormal> strictActualsToFormals = actualsToFormals.stream().filter(a -> !NonStrict.class.isAssignableFrom(a._formalType)).collect(Collectors.toList());

        Set<String> nonStrictOnlyPorts = nonStrictActualsToFormals.stream().map(a -> a._inputPort).collect(Collectors.toSet());
        nonStrictOnlyPorts.removeAll(strictActualsToFormals.stream().map(a -> a._inputPort).collect(Collectors.toSet()));

        _contsDataRetrieval = new ContinuationRuntime.ContinuationsAwareDataRetrieval(nonStrictOnlyPorts);
        super.setDataRetrieval(_contsDataRetrieval);
        super.setCallArrayConstruction(new ContinuationsAwareCallArrayConstruction(nonStrictOnlyPorts));

        _output = o -> _contsDataRetrieval.registerNewContinuations((Continuations) o, this::matchOutput);
      } else {
        // use the default
      }

      super.prepareInputSide(actualsToFormals, envArgsToFormals);
    }

    protected OutputMatch.OutputMatcher getOutputMatcher(String outPortRef) {
      OutputMatch.MATCH_TYPE matchType = findOutputMatchType(outPortRef);
      if (matchType != OutputMatch.MATCH_TYPE.ALL && _contsDataRetrieval != null) {
        return StatefulFunction.getOutputMatcher(_funcOp.getFunctionType(), Object[].class, matchType);
      } else
        return super.getOutputMatcher(outPortRef);
    }

    protected boolean isExecuteCall() {
      boolean execCall = super.isExecuteCall();
      // TODO remove this conditional call via lambdas
      if (_contsDataRetrieval != null) _contsDataRetrieval.setDrainNonStrictPorts(execCall);
      return execCall;
    }

    protected void prepareOutputSide() {
      super.prepareOutputSide();
      _pending = new Object[_outPortControls.size()][];
    }

    // we are sealing the API here because our whole continuations stuff relies on the loop being executed as defined!
    public final Object[] matchInputSchema() {
      return matchInputSchema(this);
    }

    // FIXME I really don't like this game here. We are finalizing this just to introduce another way of changing this method.
    public final boolean matchOutputSchema(Object results) {
      return _output.apply(results);
    }

    public boolean matchOutput(Object results) {
      boolean backoff = false;
      for (int i = 0; i < _outPortControls.size(); i++) {
        Object[] outRefPrep = _outPortControls.get(i);
        OutputPortControl outPort = (OutputPortControl) outRefPrep[0];
        Object currentResults = super.matchOutputPort(outPort, outRefPrep, results);
        Object finalResults = finalizePartial(currentResults, i);
        if (finalResults != null) {
          backoff |= super.sendResults(outPort, finalResults);
        } else {
          // no data returned
        }
      }
      return backoff;
    }


    // FIXME state needs to be saved and restored properly!
    public Object finalizePartial(Object result, int stateRef) {
      // FIXME this condition can be checked at compile-time! just check the return type of the function.
      if (result instanceof Object[]) {
        Object[] r = (Object[]) result;
        if (_pending[stateRef] != null) {
          // fill
//        Assertion.invariant(_pending[stateRef].length == r.length, "" + _pending[stateRef].length + ":" + r.length + ":" + r.getClass() + ":" + r[0]);
          assert _pending[stateRef].length == r.length;
          for (int i = 0; i < _pending[stateRef].length; i++) {
            if (_pending[stateRef][i] == DataflowFunction.Control.DROP) {
              _pending[stateRef][i] = r[i];
            } else {
              Assertion.invariant(r[i] == DataflowFunction.Control.DROP, "Incorrect operator implementation use of Control.DROP. " +
                      "Slot reassigned before complete packet was constructed!");
            }
          }
          r = _pending[stateRef];
        }

        for (Object a : r) {
          if (a == DataflowFunction.Control.DROP) {
            _pending[stateRef] = r;
            return null;
          }
        }
        _pending[stateRef] = null;
        return r;
      } else {
        if (result == DataflowFunction.Control.DROP) return null;
        else return result;
      }
    }
  }

  public static class DataflowSchemaMatcher extends AbstractDataflowSchemaMatcher {

    public DataflowSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }
  }

  public static class SourceDataflowSchemaMatcher extends AbstractDataflowSchemaMatcher {

    // TODO save state properly!
    private SourceDataRetrieval _ret = null;
    private Function<Object, Boolean> _matchOutput = null;

    public SourceDataflowSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    protected void prepareInputSide(List<ActualToFormal> actualsToFormals, List<EnvArgToFormal> envArgsToFormals) {
      super.prepareInputSide(actualsToFormals, envArgsToFormals);
      _ret = new SourceDataRetrieval(super._contsDataRetrieval);
      super.setDataRetrieval(_ret);
    }

    protected void prepareOutputSide() {
      super.prepareOutputSide();
      _matchOutput = isProvideControlData() ?
              results -> {
                Object val = ((Either) results)._data;
                if (val == DataflowFunction.Finish.DONE) {
                  _ret._done = true;
                  return true;
                } else {
                  return super.matchOutput(val);
                }
              }
              :
              results -> {
                _ret._done = true;
                return super.matchOutput(results);
              };
    }

    private boolean isProvideControlData() {
      if (Either.class.isAssignableFrom(_funcOp.getReturnType())) {
        Type[] typeArguments = ((ParameterizedType) _funcOp.getReturnType()
                .getGenericSuperclass()).getActualTypeArguments();
        return Arrays.stream(typeArguments).anyMatch(t -> DataflowFunction.Finish.class.getName().equals(t.getTypeName()));
      } else {
        return false;
      }
    }

    protected Class<?> getReturnType() {
      if (Either.class.isAssignableFrom(_funcOp.getReturnType())) {
        Type[] typeArguments = ((ParameterizedType) _funcOp.getReturnType().getGenericSuperclass()).getActualTypeArguments();
        if (!typeArguments[0].getTypeName().equals(DataflowFunction.Finish.class.getName()) && !typeArguments[1].getTypeName().equals(DataflowFunction.Finish.class.getName())) {
          return super.getReturnType();
        } else {
          Type t = typeArguments[0].getTypeName().equals(DataflowFunction.Finish.class.getName()) ? typeArguments[1] : typeArguments[0];
          assert t instanceof Class;
          return (Class) t;
        }
      } else {
        return super.getReturnType();
      }
    }

    public boolean matchOutput(Object results) {
      return _matchOutput.apply(results);
    }

    @Override
    public boolean isComputationComplete() {
      return _ret._done;
    }
  }

  private static class SourceDataRetrieval extends DataRetrieval {

    private DataRetrieval _inherited = null;
    private boolean _done = false;

    public SourceDataRetrieval(DataRetrieval inherited) {
      _inherited = inherited;
    }

    public boolean isCallDataAvailable() {
      if (_inherited != null) _inherited.isCallDataAvailable();
      return !_done;
    }
  }

  public static class TargetDataflowSchemaMatcher extends DataflowSchemaMatcher {
    public TargetDataflowSchemaMatcher(IFunctionalOperator functionalOperator, UserOperator ohuaOperator) {
      super(functionalOperator, ohuaOperator);
    }

    public boolean matchOutput(Object results) {
      return false;
    }

  }

  public abstract static class AbstractNonStrict{
    abstract protected AbstractNonStrict reset();
  }
  private static class ContinuationsAwareCallArrayConstruction extends CallArrayConstruction {
    private Map<String, IdempotentInputPortChecker> _nonStrictOnlyPortsCheckers;

    public ContinuationsAwareCallArrayConstruction(Set<String> nonStrictOnlyPorts) {
      _nonStrictOnlyPortsCheckers = nonStrictOnlyPorts.stream().collect(Collectors.toMap(s -> s, s -> new IdempotentInputPortChecker()));
    }

    protected Supplier<Object> createEnvArgRetrievalFunction(AbstractSchemaMatcher.EnvArgToFormal e) {
      Supplier<Object> envArgRetrieval = super.createEnvArgRetrievalFunction(e);
      if (NonStrict.class.isAssignableFrom(e._formalType)) {
        // type-wise is a bit weird because we box the given object into a NonStrict value.
        // EnvArgs depend on all value args, i.e., at least one.
        Optional<IdempotentInputPortChecker> fst = _nonStrictOnlyPortsCheckers.values().stream().findFirst();

        // reusing the Non-Strict object here. this works because the current implementation does not allow another call to
        // proceed until all continuations of the previous call were handled. once we change that behavior then this needs
        // to turn into an ArrayDeque.
        AbstractNonStrict ns = new NonStrict<>(envArgRetrieval,
                fst.isPresent() ? () -> fst.get().get() : () -> true);
        return fst.isPresent() ?
                () -> {
                  fst.get().reset();
                  return ns.reset();
                } :
                ns::reset;
      } else {
        return envArgRetrieval;
      }
    }

    protected Supplier<Object> createValueRetrievalFunction(AbstractSchemaMatcher.ActualToFormal a) {
      Supplier<Object> dataRetrieval = super.createValueRetrievalFunction(a);
      if (NonStrict.class.isAssignableFrom(a._formalType)) {
        if (_nonStrictOnlyPortsCheckers.containsKey(a._inputPort)) {
          IdempotentInputPortChecker checker = _nonStrictOnlyPortsCheckers.get(a._inputPort);
          if (checker._inPortChecker == null)
            checker._inPortChecker = ((ContinuationRuntime.NonStrictInputDataChecker) a._checker)._original;

          // reusing the Non-Strict object here. this works because the current implementation does not allow another call to
          // proceed until all continuations of the previous call were handled. once we change that behavior then this needs
          // to turn into an ArrayDeque.
          AbstractNonStrict ns = new NonStrict<>(dataRetrieval, checker.reset());
          return () -> {
            checker.reset();
            return ns.reset();
          };
        } else {
          // there is strict data coming from that port too, so the strict data already did the check.

          // reusing the Non-Strict object here. this works because the current implementation does not allow another call to
          // proceed until all continuations of the previous call were handled. once we change that behavior then this needs
          // to turn into an ArrayDeque.
          AbstractNonStrict ns = new NonStrict<>(dataRetrieval, () -> true);
          return ns::reset;
        }
      } else {
        return dataRetrieval;
      }
    }
  }

  /**
   * Makes sure that we do not call 'next' twice on an input port that already has data (and thereby would drop packets).
   */
  private static class IdempotentInputPortChecker implements Supplier<Boolean> {
    private boolean _hasData = false;
    private DataRetrieval.InputDataChecker _inPortChecker = null;

    private IdempotentInputPortChecker reset() {
      _hasData = false;
      return this;
    }

    @Override
    public Boolean get() {
      if (!_hasData) _hasData = !_inPortChecker.isCallDataPending();
      return _hasData;
    }
  }

}
