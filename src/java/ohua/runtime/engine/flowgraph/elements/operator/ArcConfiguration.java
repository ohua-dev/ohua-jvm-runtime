/*
 * Copyright (c) Sebastian Ertel 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public interface ArcConfiguration {
    /**
     * Default loading of pairs from a properties file with entries of the form: op.input-port = 300,
     * where 300 is the definition of the arc boundary.
     *
     * @param file
     * @return
     * @throws IOException
     */
    static Map<String, ArcConfiguration> load(String file) throws IOException {
        Properties props = new Properties();
        props.load(new FileReader(file));
        return props.entrySet().stream().collect(Collectors.toMap(
                (Map.Entry k) -> k.getKey().toString(),
                (Map.Entry v) -> new ArcConfiguration() {
                    public void configure(Arc arc) {
                        arc.setArcBoundary(Integer.parseInt(v.getValue().toString()));
                    }
                }));
    }

    void configure(Arc arc);
}
