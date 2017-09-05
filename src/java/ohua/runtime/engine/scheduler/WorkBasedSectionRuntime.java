/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.RuntimeProcessConfiguration;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.WorkBasedOperatorRuntime;
import ohua.runtime.engine.sections.ISectionRuntime;
import ohua.runtime.engine.sections.AbstractSection;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sertel on 1/26/17.
 */
public class WorkBasedSectionRuntime implements ISectionRuntime<Object> {

  private AbstractSection _section;
  protected WorkBasedOperatorScheduler _opScheduler;

  protected WorkBasedSectionRuntime(AbstractSection section, Map<OperatorCore, WorkBasedOperatorRuntime> opRuntimes,
                                    RuntimeProcessConfiguration config){
    _section = section;
    Set<WorkBasedOperatorRuntime> sectionOpRuntimes =_section.getOperators()
            .stream()
            .map(opRuntimes::get)
            .collect(Collectors.toSet());
    _opScheduler = new WorkBasedOperatorScheduler(sectionOpRuntimes, config);
    _opScheduler.initialize();
  }

  @Override
  public void startNewSystemPhase() {
    // nothing to be done
  }

  @Override
  public Object call() throws Exception {
    _opScheduler.runExecutionStep();
    return this;
  }
}
