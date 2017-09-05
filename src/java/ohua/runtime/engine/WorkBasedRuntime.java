package ohua.runtime.engine;

import ohua.runtime.engine.flowgraph.elements.operator.*;
import ohua.runtime.engine.scheduler.AbstractScheduler;
import ohua.runtime.engine.scheduler.WorkBasedAsynchronousArc;
import ohua.runtime.engine.scheduler.WorkBasedTaskScheduler;
import ohua.runtime.engine.sections.SectionGraph;

/**
 * Created by sertel on 1/28/17.
 */
public class WorkBasedRuntime extends AbstractRuntime {

  public WorkBasedRuntime(RuntimeProcessConfiguration config) {
    super(config);
  }

  @Override
  protected AbstractOperatorRuntime createOperatorRuntime(OperatorCore op) {
    return new WorkBasedOperatorRuntime(op, _runtimeConfiguration);
  }

  @Override
  protected AbstractScheduler createScheduler() {
    return new WorkBasedTaskScheduler();
  }

  @Override
  protected AbstractArcImpl createArcImpl(Arc arc) {
    return new WorkBasedAsynchronousArc();
  }
}
