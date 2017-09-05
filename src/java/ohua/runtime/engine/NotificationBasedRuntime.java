/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.scheduler.AbstractScheduler;
import ohua.runtime.engine.sections.SectionGraph;
import ohua.runtime.engine.sections.SectionScheduler;

import java.io.IOException;
import java.util.Map;

/**
 * Created by sertel on 1/26/17.
 */
public class NotificationBasedRuntime extends AbstractRuntime {

  protected NotificationBasedRuntime(RuntimeProcessConfiguration config) {
    super(config);
  }

  @Override
  protected AbstractOperatorRuntime createOperatorRuntime(OperatorCore op) {
    return new NotificationBasedOperatorRuntime(op, _runtimeConfiguration);
  }

  @Override
  protected AbstractScheduler createScheduler() {
    return new SectionScheduler();
  }

  protected void initializeArcs(SectionGraph graph) {
    super.initializeArcs(graph);

    graph.getAllArcs().stream().forEach(a -> a.setImpl( new AsynchronousArcImpl(a)));

    // apply custom arc config
    Object arcConfig = _runtimeConfiguration.getArcConfiguration();
    if(arcConfig == null) return;

    try {
      Map<String, ArcConfiguration> arcSpec = arcConfig instanceof String ?
              ArcConfiguration.load((String) arcConfig) : (Map<String, ArcConfiguration>) arcConfig;
      for(Map.Entry<String, ArcConfiguration> entry : arcSpec.entrySet()) {
        // find the arc
        String[] ref = FlowGraph.parsePortReference(entry.getKey());
        OperatorCore op = graph.findOperator(ref[0]);
        // configure
        entry.getValue().configure(op.getInputPort(ref[1]).getIncomingArc());
      }
    }catch(IOException e){
      // report but swallow
      e.printStackTrace();
    }
  }

  @Override
  protected AbstractArcImpl createArcImpl(Arc arc) {
    return new AsynchronousArcImpl(arc);
  }
}
