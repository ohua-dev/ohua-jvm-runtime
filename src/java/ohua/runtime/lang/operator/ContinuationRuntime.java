/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang.operator;

import ohua.runtime.engine.daapi.InputPortControl;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.lang.Continuations;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by sertel on 9/20/16.
 */
public final class ContinuationRuntime {

  // state to optimize execution
  private ArrayDeque<Continuation> _emptyAtConts = new ArrayDeque<>();
  private ArrayDeque<ConsumingContinuation> _emptyConsumingConts = new ArrayDeque<>();

  private ContinuationRuntime() {
  }

  public static <T> PendingValue<T> createPendingValue(Supplier<T> retrieval, Supplier<Boolean> check) {
    return new PendingValue<>(retrieval, check);
  }

  public static abstract class AbstractContinuations {
    protected List<AbstractContinuation> _conts = null;
    protected Object _result = null;
    protected boolean _finished = false;
    private ContinuationRuntime _runtime = null;

    protected AbstractContinuations(){
      // using a linked list would make the remove(idx) call in ContinuationHandler.get() fast
      // but it's construction is super expensive.
      _conts = new ArrayList<>();
      _runtime = new ContinuationRuntime();
    }

    protected AbstractContinuations(Object result){
      _result = result;
      _finished = true;
    }

    protected final <T> AbstractContinuation<T> createContinuation(ContinuationRuntime.PendingValue<T> pendingVal, Function<T, Continuations> continuation) {
      return _runtime._emptyAtConts.isEmpty() ?
              _runtime.new Continuation<>(pendingVal, continuation) :
              _runtime._emptyAtConts.poll().set(pendingVal, continuation);
    }

    protected final <T> AbstractContinuation<T> createConsumingContinuation(ContinuationRuntime.PendingValue<T> pendingVal, Consumer<T> continuation) {
      return _runtime._emptyConsumingConts.isEmpty() ?
              _runtime.new ConsumingContinuation<>(pendingVal, continuation) :
              _runtime._emptyConsumingConts.poll().set(pendingVal, continuation);
    }
  }

  private static abstract class AbstractContinuation<T> {

    protected PendingValue<T> _pendingVal = null;
    private boolean _isReady = false;

    private AbstractContinuation(PendingValue<T> pendingVal) {
      _pendingVal = pendingVal;
    }

    public boolean isReady() {
      // make this call idempotent!
      if (!_isReady) _isReady = _pendingVal._check.get();
      return _isReady;
    }

    protected final void reset(){
      _isReady = false;
      _pendingVal = null;
    }
  }

  public final static class PendingValue<T> {
    Supplier<T> _retrieval = null;
    Supplier<Boolean> _check = null;

    private PendingValue(Supplier<T> retrieval, Supplier<Boolean> check) {
      _retrieval = retrieval;
      _check = check;
    }
  }

  public static class ContinuationsAwareDataRetrieval extends DataRetrieval {

    private Set<String> _nonStrictOnlyInputPorts = null;
    private List<InputDataChecker> _strictInputPorts = new LinkedList<>();
    private List<InputDataChecker> _nonStrictPorts = new ArrayList<>(); // TODO use Set to make the lookup faster
    private Supplier<Boolean> _continuationHandler = () -> true; // first default to catch the case when matchInputSchema is called the first time around

    // state
    private List<InputDataChecker> _pendingNonStrictPorts = new ArrayList<>();
    private ContinuationHandler _cHandler = new ContinuationHandler();

    public ContinuationsAwareDataRetrieval(Set<String> nonStrictOnlyInputPorts) {
      _nonStrictOnlyInputPorts = nonStrictOnlyInputPorts;
    }

    @Override
    protected InputDataChecker create(String inPortRef, UserOperator ohuaOp) {
      InputDataChecker retrieval = super.create(inPortRef, ohuaOp);
      if (isNonStrictOnlyPort(inPortRef)) {
        _nonStrictPorts.add(retrieval);
        // is passed into the create NonStrict object because it is stored in ActualToFormal._checker!
        return new NonStrictInputDataChecker(
                ohuaOp.getDataLayer().getInputPortController(inPortRef),
                retrieval,
                () -> _strictInputPorts.stream().allMatch(i -> i.hasSeenLastPacket() && i.isComputationComplete()), // EOS received and Out-of-context ports are done.
                () -> _strictInputPorts.size());
      } else {
        _strictInputPorts.add(retrieval);
        return retrieval;
      }
    }

    public boolean isNonStrictOnlyPort(String inPortRef) {
      return _nonStrictOnlyInputPorts.contains(inPortRef);
    }

    @Override
    protected boolean isCallDataAvailable() {
      if (isNonStrictPortsDrained()) {
        boolean contsDone = _continuationHandler.get();
        if (contsDone) {
          boolean dataAvailable = super.isCallDataAvailable();
          return dataAvailable;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }

    /**
     * The call executed and therefore continuations were returned which need to be handled.
     *
     * @param conts
     * @param applyResult
     * @return
     */
    public boolean registerNewContinuations(Continuations conts, Consumer applyResult) {
      // TODO I swallow the return value of the matchOutputSchema function here. not sure if this is too bad though.
      _continuationHandler = _cHandler.set(conts, applyResult);
      return false;// enforce a new cycle which then takes on these registered continuations.
    }

    /**
     * The context port may decide to not execute this call, so make sure in this case the non-strict ports are drained.
     *
     * @param execCall
     * @return
     */
    public final boolean setDrainNonStrictPorts(boolean execCall) {
      assert _pendingNonStrictPorts.isEmpty();
      if (!execCall) _pendingNonStrictPorts.addAll(_nonStrictPorts);
      return execCall;
    }

    private boolean isNonStrictPortsDrained() {
//      System.out.println("DRAINING: " + _nonStrictOnlyInputPorts + " current: " + _pendingNonStrictPorts.size());
      List<InputDataChecker> n = new ArrayList<>(_pendingNonStrictPorts.size());
      for (int i = 0; i < _pendingNonStrictPorts.size(); i++) { // fastest iteration technique
        InputDataChecker idc = _pendingNonStrictPorts.get(i);
        if (!(idc.hasSeenLastPacket() && idc.isComputationComplete())) {
          n.add(idc);
        }
      }
      _pendingNonStrictPorts = n;
      return _pendingNonStrictPorts.isEmpty();
    }

    // TODO removed the collect call. -> faster but not as fast as the above.
//    private boolean isNonStrictPortsDrained() {
////      System.out.println("DRAINING: " + _nonStrictOnlyInputPorts + " current: " + _pendingNonStrictPorts.size());
//      List n = new ArrayList<>(_pendingNonStrictPorts.size());
//      _pendingNonStrictPorts.stream()
//              .filter(i -> !(i.hasSeenLastPacket() && i.isComputationComplete()))
//              .forEach(n::add);
//      _pendingNonStrictPorts = n;
//      return _pendingNonStrictPorts.isEmpty();
//    }

    // TODO better style but the collect call is a bottleneck
//    private boolean isNonStrictPortsDrained() {
////      System.out.println("DRAINING: " + _nonStrictOnlyInputPorts + " current: " + _pendingNonStrictPorts.size());
//      _pendingNonStrictPorts = _pendingNonStrictPorts.stream()
//              .filter(i -> !(i.hasSeenLastPacket() && i.isComputationComplete()))
//              .collect(Collectors.toList());
//      return _pendingNonStrictPorts.isEmpty();
//    }

  }

  protected static class NonStrictInputDataChecker extends DataRetrieval.InputDataChecker {

    protected DataRetrieval.InputDataChecker _original = null;
    private Supplier<Boolean> _areStrictPortsDone = null;
    private Supplier<Integer> _numStrictPorts = null;

    public NonStrictInputDataChecker(InputPortControl inPort, DataRetrieval.InputDataChecker original, Supplier<Boolean> areStrictPortsDone, Supplier<Integer> numStrictPorts) {
      super(inPort);
      _original = original;
      _areStrictPortsDone = areStrictPortsDone;
      _numStrictPorts = numStrictPorts;
    }

    @Override
    protected boolean isCallDataPending() {
      // generally, data is "always" available. the real check is performed in NonStrict.
      return false;
    }

    @Override
    public boolean isComputationComplete() {
      return _original.isComputationComplete();
    }
  }

  private static class ContinuationHandler implements Supplier<Boolean> {

    private List<Continuations> _continuations = new ArrayList<>();
    private Consumer _applyResult = null;

    private Supplier<Boolean> set(Continuations continuations, Consumer applyResult) {
      assert _continuations.isEmpty();
      _continuations.add(continuations);
      _applyResult = applyResult;
      return this;
    }

    /**
     * @return true, if all continuations have been processed. false, otherwise.
     */
    public Boolean get() {
//      int outer = 0;
//      int inner = 0;
      int i = 0;
      while (i < _continuations.size()) {
//        outer++;
        Continuations conts = _continuations.get(i);
        if (conts._finished) {
          _applyResult.accept(conts._result);
          _continuations.remove(i); // kick out done ones (outer loop)
        } else {
          int j = 0;
          while (j < conts._conts.size()) { // loop for continuations
//            inner++;
            AbstractContinuation c = conts._conts.get(j);
            if (c.isReady()) {
              if (c instanceof Continuation) {
                Continuations res = ((Continuation) c).get();
                if (res._finished)
                  _applyResult.accept(res._result);
                else
                  _continuations.add(res); // add (outer loop)
              } else if (c instanceof ConsumingContinuation) {
                ((ConsumingContinuation) c).accept();
              } else {
                Assertion.impossible("Unknown continuation type: " + c.getClass());
                throw new RuntimeException();
              }
              conts._conts.remove(j); // kick the done ones out (inner loop)
            } else {
              j++; // leave the none-ready ones in (inner loop)
            }
          }

          if (!conts._conts.isEmpty()) {
            // leave them inside (outer loop)
            i++;
          } else {
            // kick them out because all are done (outer loop)
            _continuations.remove(i);
          }
        }
      }
//      System.out.println("Outer: " + outer + " Inner: " + inner);
      return _continuations.isEmpty();
    }


    /**
     * TODO This code was a blocker in this implementation particularly because of the ReferencePipeline.collect() method that took a substantial amount of time.
     *      Re-enable this code once they have fixed this performance bottleneck in later Java versions.
     * @return
     */
//    public Boolean get() {
//      // FIXME the isReady call will be expensive for the case where no data is available because it always does a poll on the port.
//      while (_continuations.stream().anyMatch(conts ->
//              conts._finished
//                      || conts._conts.stream().anyMatch(AbstractContinuation::isReady))) {
//        _continuations = _continuations.stream()
//                .flatMap(conts -> {
//                  if (conts._finished) {
//                    _applyResult.accept(conts._result);
//                    return Stream.empty();
//                  } else {
//                    return Stream.of(conts);
//                  }
//                })
//                .flatMap(conts -> {
//                          Stream<AbstractContinuation> ready = conts._conts.stream().filter(AbstractContinuation::isReady);
//                          List<AbstractContinuation> notReady = conts._conts.stream()
//                                  .filter(c -> !c.isReady())
//                                  .collect(Collectors.toCollection(() -> new ArrayList<>(conts._conts.size())));
//
//                          List<Continuations> newConts = ready.flatMap(c -> {
//                            if (c instanceof Continuation) {
//                              Continuations res = ((Continuation) c).get();
//                              return Stream.of(res);
//                            } else if (c instanceof ConsumingContinuation) {
//                              ((ConsumingContinuation) c).accept();
//                              return Stream.empty();
//                            } else {
//                              Assertion.impossible("Unknown continuation type: " + c.getClass());
//                              throw new RuntimeException();
//                            }
//                          }).collect(Collectors.toCollection(() -> new ArrayList<>(conts._conts.size())));
//
//                          if (!notReady.isEmpty()) {
//                            conts._conts = notReady;
//                            newConts.add(0, conts);
//                          }
//                          return newConts.stream();
//                        }
//                ).collect(Collectors.toCollection(LinkedList::new));
//      }
//
//      return _continuations.isEmpty();
//    }
  }

  private final class Continuation<T> extends ContinuationRuntime.AbstractContinuation<T> implements Supplier<Continuations> {

    private Function<T, Continuations> _continuation = null;

    private Continuation(ContinuationRuntime.PendingValue<T> pendingVal, Function<T, Continuations> continuation) {
      super(pendingVal);
      _continuation = continuation;
    }

    private Continuation set(ContinuationRuntime.PendingValue<T> pendingVal, Function<T, Continuations> continuation) {
      _pendingVal = pendingVal;
      _continuation = continuation;
      return this;
    }

    public Continuations get() {
      Continuations cs = _continuation.apply(super._pendingVal._retrieval.get());
      _emptyAtConts.add(_reset());
      return cs;
    }

    private Continuation _reset(){
      super.reset();
      _continuation = null;
      return this;
    }

  }

  private final class ConsumingContinuation<T> extends ContinuationRuntime.AbstractContinuation<T> {

    private Consumer<T> _continuation = null;

    private ConsumingContinuation(ContinuationRuntime.PendingValue<T> pendingVal, Consumer<T> continuation) {
      super(pendingVal);
      _continuation = continuation;
    }

    private ConsumingContinuation set(ContinuationRuntime.PendingValue<T> pendingVal, Consumer<T> continuation) {
      _pendingVal = pendingVal;
      _continuation = continuation;
      return this;
    }

    public void accept() {
      _continuation.accept(super._pendingVal._retrieval.get());
      _emptyConsumingConts.add(_reset());
    }

    private ConsumingContinuation _reset(){
      super.reset();
      _continuation = null;
      return this;
    }
  }

}
