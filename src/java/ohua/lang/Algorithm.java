/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.lang;

import clojure.lang.IRecord;
import clojure.lang.Symbol;
import ohua.runtime.lang.operator.DataflowFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


/**
 * Created by sertel on 6/8/16.
 */
public class Algorithm {

  public interface IAlgorithm extends IRecord {
    Symbol mkSymbol();
  }

  public static class AlgoInStrict {
    /**
     * The first argument is associated state that keeps the information
     * of the algorithm that this function is an entry to.
     */
    @defsfn(useJavaVarArgsSemantics = false)
    public Object[] algoInStrict(IAlgorithm algo, Object... args) {
      return args;
    }
  }

  public static class AlgoInVoid {
    /**
     * The first argument is associated state that keeps the information
     * of the algorithm that this function is an entry to.
     */
    @defsfn
    public boolean algoInVoid(IAlgorithm algo) {
      return true; // context signal
    }
  }

  public static class AlgoOut {
    @defsfn
    public Object algoOut(Object args) {
      return args;
    }
  }

  private static class ArgContinuation implements Function<Object, Continuations> {
    private Continuations _finish = Continuations.empty();

    private Object[] _result;
    private int _argIdx;
    private int _argsLength;

    private ArgContinuation(Integer argIdx, Integer argsLength) {
      _argIdx = argIdx;
      _argsLength = argsLength;
      _result = new Object[_argsLength];
      Arrays.fill(_result, DataflowFunction.Control.DROP);
    }

    @Override
    public Continuations apply(Object v) {
      _result[_argIdx] = v;
      return _finish.done(_result);
    }
  }

  public static class AlgoInNonStrict {
    // this works only because all continuations must have been processed before the next call is being created.
    private Continuations _continuations = Continuations.empty();
    private List<ArgContinuation> _argConts = new ArrayList<>();

    @defsfn
    @DataflowFunction
    public Continuations algoIn(@Deprecated IAlgorithm alg, // TODO This is not used and does not work anymore if we don't register algorithms globally. If we want to pass this in here we need a different way of doing so (currently passing in nil)
                                NonStrict<Object>... algoArgs) {
      if (_argConts.isEmpty())
        for (int i = 0; i < algoArgs.length; i++)
          _argConts.add(new ArgContinuation(i, algoArgs.length));

      for (int i = 0; i < algoArgs.length; i++)
        _continuations.at(algoArgs[i], _argConts.get(i));

      return _continuations;
    }
  }

}
