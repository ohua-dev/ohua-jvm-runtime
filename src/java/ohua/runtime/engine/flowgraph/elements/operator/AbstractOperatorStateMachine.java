/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import ohua.runtime.engine.OperatorEvents;
import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.exceptions.Assertion;

import java.io.Serializable;

/**
 * Created by sertel on 2/1/17.
 */
public abstract class AbstractOperatorStateMachine<T extends AbstractOperatorRuntime> {

  protected T _operatorRuntime = null;

  protected AbstractOperatorStateMachine(T runtime) {
    _operatorRuntime = runtime;
  }

  protected final void runTransition() {
    switch (_operatorRuntime.getOperatorState()) {
      case INIT:
        Transition t = init();
        t.apply(_operatorRuntime);
        break;
      case WAITING:
        Transition.WAITING_to_EXECUTING_META_DATA.apply(_operatorRuntime);
        break;
      case EXECUTING_META_DATA:
        t = executingMetaData();
        t.apply(_operatorRuntime);
        break;
      case EXECUTING:
//        System.out.println("Executing op: " + _operatorRuntime.getOperatorName());
        Transition.LEAVE_OHUA_ENGINE.apply(_operatorRuntime);
        _operatorRuntime.raiseOperatorEvent(OperatorEvents.USER_OPERATOR_EXECUTION);
        _operatorRuntime.executeOperator().run(_operatorRuntime.getOp().getOperatorAlgorithm());
        _operatorRuntime.raiseOperatorEvent(OperatorEvents.USER_OPERATOR_RETURNED);
        Transition.RETURN_TO_OHUA_ENGINE.apply(_operatorRuntime);
        Transition.EXECUTING_to_EXECUTING_EPILOGUE.apply(_operatorRuntime);
        break;
      case EXECUTING_USER_OP:
        // "concurrent" state to EXECUTING -> no transition
        executingUserOp();
        break;
      case EXECUTING_EPILOGUE:
        t = executingEpilogue();
        t.apply(_operatorRuntime);
        break;
      case FINISHING_COMPUTATION:
        Transition.FINISHING_COMPUTATION_to_EXECUTING.apply(_operatorRuntime);
        break;
      case WAITING_FOR_COMPUTATION:
        // might be a dangling call or an activation. only peek the input ports to find out and then transition.
        // otherwise stay in this state.
        t = waitingForComputation();
        t.apply(_operatorRuntime);
        break;
      case CLEAN_UP:
        cleanUp();
        Transition.FINALIZE.apply(_operatorRuntime);
        break;
      case DONE:
        // we should never schedule operators that have finished their processing
        // assert false : "Never schedule operators that are in DONE state!";
        // In fact this can happen in the multi-threading scenario where this operator is the
        // first of a section. The last one of the previous section pushes this operator back
        // into the operator scheduling queue although it is still running and finishes with
        // this run. (Think of an output section for example!)
        Transition.DONE.apply(_operatorRuntime);
    }
  }

  protected Transition waitingForComputation(){
    return Transition.EXECUTE_NEW_COMPUTATION;
  }

  protected final Transition init(){
    prepareInputPorts();
    _operatorRuntime.executeOperator().prepare(_operatorRuntime.getOp().getOperatorAlgorithm());
    _operatorRuntime.getOp().getOutputPorts().forEach(OutputPort::deactivate);
    return Transition.INIT_to_WAITING_FOR_COMPUTATION;
  }

  protected Transition executingEpilogue(){
    // find the resulting state
    if (isTearDown()) {
      // This means we really want to tear down the flow.
      return Transition.FINISH_ALL_PROCESSING;
    } else if (isComputationComplete()) {
      /*
       * Note that we do enter only once here in the round when we retrieved the EOS.
       * Every subsequent round behaves as if we are blocked (because the output ports are
       * closed). This is ok though because we do not want to keep calling
       * finishOutputPorts().
       */

      // This is a bit weaker than tear down. It just says that we finished the computation
      // but keep the flow/process alive in order for successive computations to come.

      _operatorRuntime.finishOutputPorts();
//      System.out.println("Done: " + _operatorRuntime.getOp().getOperatorName());
      return Transition.WAIT_FOR_NEW_COMPUTATION;
    } else if (_operatorRuntime.isInputComplete()
            && !_operatorRuntime.getOp().isSystemComponent()) {
      return Transition.EXECUTING_EPILOGUE_to_FINISHING_COMPUTATION;
    } else {
      return Transition.EXECUTING_EPILOGUE_to_WAITING;
    }
  }

  protected Transition executingMetaData(){
    _operatorRuntime.handleMetaData();
    return _operatorRuntime.getProcessState() == SystemPhaseType.TEARDOWN ?
            Transition.EXECUTING_META_DATA_to_EXECUTING_EPILOGUE :
            Transition.EXECUTING_META_DATA_to_EXECUTING;
  }

  protected void executingUserOp(){
    // allow for meta data processing and operator activation to be performed when the user
    // operator is executing -> NO STATE TRANSITION!

    // only a certain type of marker is allowed to be processed here. basically the
    // marker must be data independent, e.g. a Fast Traveler.
    _operatorRuntime.skimMetaData(true);

    _operatorRuntime.deactivate();
  }

  protected void cleanUp(){
    _operatorRuntime.executeOperator().cleanup(_operatorRuntime.getOp().getOperatorAlgorithm());
    _operatorRuntime.closeOutputPorts();
  }

  private boolean isComputationComplete() {
    return _operatorRuntime.isComputationComplete();
  }

  private boolean isTearDown() {
    return _operatorRuntime.isTearDown();
  }

  protected void prepareInputPorts() {
    // prepare all input ports
    _operatorRuntime.getOp().getInputPorts().forEach(i -> {
      // FIXME make this a single function on the ports
      i.initialize();
      i.initComplete();
  });

    if (_operatorRuntime.getOp().getOutputPorts().isEmpty()) {
      // the MetaTargetOperator needs this hook
      _operatorRuntime.getOp().getOperatorAdapter().prepareInputPorts();
    }

  }

  public enum OperatorState implements Serializable {
    // Initialization states
    INIT,

    // Computation states
    EXECUTING_META_DATA,
    EXECUTING,
    EXECUTING_USER_OP,
    EXECUTING_EPILOGUE,
    WAITING,
    FINISHING_COMPUTATION,

    // Intermediate state
    WAITING_FOR_COMPUTATION,

    // Shutdown states
    CLEAN_UP,
    DONE
  }

  protected enum Transition {
    INIT_to_WAITING(OperatorState.INIT,
            OperatorState.WAITING, false),
    INIT_to_WAITING_FOR_COMPUTATION(OperatorState.INIT,
            OperatorState.WAITING_FOR_COMPUTATION, false),

    WAITING_to_EXECUTING_META_DATA(OperatorState.WAITING,
            OperatorState.EXECUTING_META_DATA, true),

    EXECUTING_META_DATA_to_EXECUTING(OperatorState.EXECUTING_META_DATA,
            OperatorState.EXECUTING, true),
    EXECUTING_META_DATA_to_EXECUTING_EPILOGUE(OperatorState.EXECUTING_META_DATA,
            OperatorState.EXECUTING_EPILOGUE, true),
    FINISHING_COMPUTATION_to_EXECUTING(OperatorState.FINISHING_COMPUTATION,
            OperatorState.EXECUTING, true),

    EXECUTING_to_EXECUTING_EPILOGUE(OperatorState.EXECUTING,
            OperatorState.EXECUTING_EPILOGUE, true),
    EXECUTING_EPILOGUE_to_WAITING(OperatorState.EXECUTING_EPILOGUE,
            OperatorState.WAITING, false),
    EXECUTING_EPILOGUE_to_FINISHING_COMPUTATION(OperatorState.EXECUTING_EPILOGUE,
            OperatorState.FINISHING_COMPUTATION, false),

    WAIT_FOR_NEW_COMPUTATION(OperatorState.EXECUTING_EPILOGUE,
            OperatorState.WAITING_FOR_COMPUTATION, false),
    EXECUTE_NEW_COMPUTATION(OperatorState.WAITING_FOR_COMPUTATION,
            OperatorState.EXECUTING_META_DATA, true), // marker-based
    INVALID_RUN(OperatorState.WAITING_FOR_COMPUTATION,
            OperatorState.WAITING_FOR_COMPUTATION, false),

    FINISH_ALL_PROCESSING(OperatorState.EXECUTING_EPILOGUE, OperatorState.CLEAN_UP, true),

    FINALIZE(OperatorState.CLEAN_UP, OperatorState.DONE, true),
    DONE(OperatorState.DONE, OperatorState.DONE, false),

    LEAVE_OHUA_ENGINE(OperatorState.EXECUTING, OperatorState.EXECUTING_USER_OP, true),
    RETURN_TO_OHUA_ENGINE(OperatorState.EXECUTING_USER_OP, OperatorState.EXECUTING, true);

    private OperatorState _from, _to = null;
    private boolean _runAnotherTransition;

    Transition(OperatorState fromState, OperatorState toState, boolean runAnotherTransition) {
      _from = fromState;
      _to = toState;
      _runAnotherTransition = runAnotherTransition;
    }

    private void apply(AbstractOperatorRuntime operator) {
      Assertion.invariant(operator.getOperatorState() == _from);
      operator.setOperatorState(_to);
      operator.setActive(_runAnotherTransition);
    }
  }

}
