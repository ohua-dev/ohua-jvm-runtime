/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.lang;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.lang.operator.Stats;
import ohua.util.Tuple;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by sertel on 11/17/16.
 */
public class RuntimeStatistics {

  public static class AccumulatedStats implements Stats.IStatsCollector {
    protected float _mvgAvg = 0;
    protected long _accumulated = 0;
    protected long _max = 0;
    protected long _min = Long.MAX_VALUE;
    private long _start = 0;
    private long _n = 0;

    public void begin() {
      _start = System.nanoTime();
    }

    public void end() {
      long stop = System.nanoTime();
      long execTime = (stop - _start);
      _accumulated += execTime;
      // FIXME unnecessary! collect only accumulated and n. calculate this in the script!
      _mvgAvg = (execTime + (_n * _mvgAvg)) / (_n + 1);
      _n++;
      if (execTime < _min) _min = execTime;
      if (execTime > _max) _max = execTime;
    }

    public void log(Appendable resource) throws IOException {
      resource.append(generateJSonResult());
    }

    private String generateJSonResult() {
      return "{ \"avg\" : " + _mvgAvg + ", " +
              "\"total\" : " + (_accumulated / 1000000.0) + ", " +
              "\"min\" : " + _min + ", " +
              "\"max\" : " + _max + " }";
    }
  }

  public static class TracedStats implements Stats.IStatsCollector {

    private static final String OPEN_BRACE = "[ ";
    private static final String CLOSE_BRACE = " ]";
    private static final String COMMA = ", ";
    public static int SAMPLING_INTERVAL = 0;
    private LinkedList<Tuple<Long, Long>> _trace = new LinkedList<>();
    private Tuple<Long, Long> _current = null;

    public void begin() {
      _current = new Tuple<>(System.nanoTime(), 0L);
    }

    public void end() {
      _current._t = System.nanoTime();
      _trace.add(_current);
    }

    public void log(Appendable resource) throws IOException {
      resource.append(OPEN_BRACE);
      int i = 1;
      for (Tuple<Long, Long> t : _trace) {
        resource.append(OPEN_BRACE);
        resource.append(Long.toString(t.first()));
        resource.append(COMMA);
        resource.append(Long.toString(t.second()));
        resource.append(CLOSE_BRACE);
        if (i != _trace.size()) resource.append(COMMA);
        i++;
      }
      resource.append(CLOSE_BRACE);
    }
  }

  public static class GlobalStatsLogger {
    public static Consumer<String> LOG_STATS = totalLog -> {
    };

    public static void log(FlowGraph graph) {
      StringBuilder b = new StringBuilder();
      b.append("[");
      List<Stats.ILoggable> loggableOps = graph.getContainedGraphNodes().stream().map(OperatorCore::getOperatorAlgorithm)
              .filter(op -> op instanceof Stats.ILoggable)
              .map(op -> (Stats.ILoggable) op)
              .collect(Collectors.toList());
      try {
        IntStream.range(0, loggableOps.size() - 1)
                .mapToObj(i -> loggableOps.get(i))
                .forEach(s -> {
                  try {
                    s.logStats(b);
                  } catch (IOException i) {
                    throw new RuntimeException(i);
                  }
                  b.append(", \n");
                });
        loggableOps.get(loggableOps.size() - 1).logStats(b);
        b.append("]");
        LOG_STATS.accept(b.toString());
      } catch (Throwable t) {
        System.err.println("Logging failed: (swallowing)");
        System.err.println(t.getMessage());
      }

    }
  }
}
