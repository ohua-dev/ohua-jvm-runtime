/*
 * ohua : Backend.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.link;

import ohua.runtime.engine.flowgraph.elements.operator.IOperatorFactory;
import ohua.runtime.lang.operator.StatefulFunction;

import java.util.Set;

/**
 * Created by justusadam on 01/12/2016.
 */
public interface SFNBackend {
    void initialize();

    Set<String> listNamespace(String namespace);

    boolean exists(String namespace, String functionName);

    IOperatorFactory getOperatorFactory();

    StatefulFunction asStatefulFunction(String namespace, String functionName);
}
