/*
 * Copyright (c) Sebastian Ertel 2013. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.exceptions;

public class CompilationException extends Exception {
  private CAUSE _cause = null;
  private String _opId = null;
  public CompilationException(CAUSE cause) {
    super(cause._msg);
    _cause = cause;
  }

  public CompilationException(CAUSE cause, String opId) {
    super(cause._msg + " Operator ID = " + opId + ".");
    _cause = cause;
    _opId = opId;
  }

  public CompilationException(CAUSE cause, String opId, Exception e) {
    super(cause._msg + " Operator ID = " + opId + ".\nDetails: " + e.getMessage());
    _cause = cause;
    _opId = opId;
  }

  public CAUSE getCauze() {
    return _cause;
  }

  public String getOpId() {
    return _opId;
  }

  public enum CAUSE {
    EXCEPTION_COLLECTION("Compilation failed due to the following reasons:"),

    UNSUPPORTED_OPERATOR("The operator does not enable compile-time usage!"),
    NO_METHOD_FOUND("Operator algorithm not defined."),
    METHOD_AMBIGUITY("Multiple operator algorithm implementations detected."),
    SCHEMA_INCOMPATIBLE("The schema of the function is incompatible with the provided types!"),
    TOO_FEW_OPERATORS("There should be used at least two operators."),

    // shared memory
    MEMORY_LEAK_DETECTED("The operator leaks state!"),
    SHARED_DEPENDENCY_DETECTED("The operator's result is used by more than one operator!"),

    ILLEGAL_OPERATOR_ARGUMENT("Illegal argument to operator detected"),

    // Linking exceptions
    NO_SUCH_FUNCTION("No such function exists."),

    // analysis exceptions
    UNCLEAR_CODE_DETECTED("Please restructure your code such that the compiler can analyze it properly."),

    // matching exceptions
    ARITY_TOO_FEW("Too few arguments assigned to function."),
    ARITY_TOO_MANY("Too many arguments assigned to function."),

    // arg list exceptions
    ARGUMENT_LIST_GAP("We currently do not support mixing locals and global (environment) variables in argument lists. Please make sure all global variables come last."),
    ENVIRONMENT_ARGS_ASSIGNED_TO_SAME_SLOT("More than one environment argument assigned to one formal input slot. This is not possible by definition of the language but may happen due to dataflow IR rewrites or is a compiler bug. Please report!"),
    ARGS_ASSIGNED_TO_SAME_SLOT("More than one argument assigned to one formal input slot. This is not possible by definition of the language but may happen due to dataflow IR rewrites or is a compiler bug. Please report!"),
    ENV_ARG_ASSIGNED_TO_ALREADY_ASSIGEND_SLOT("Environment argument assigned to slot that is already bound. This is not possible by definition of the language but may happen due to dataflow IR rewrites or is a compiler bug. Please report!"),

    // dataflow function support
    WRONG_RETURN_TYPE("Dataflow Functions with NonStrict arguments must have return type Continuations.");

    private String _msg = null;

    private CAUSE(String msg) {
      _msg = msg;
    }
  }
}
