/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.scheduler;

import ohua.runtime.engine.SystemPhaseType;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.SectionGraph;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Created by sertel on 1/21/17.
 */
public interface IRuntime {
  void launch(Set<OperatorCore> ops);

  void start(SystemPhaseType systemPhase);
  void teardown();

  void onDone(Consumer<Optional<Throwable>> onDone);

  void activate(OperatorCore op);
}
