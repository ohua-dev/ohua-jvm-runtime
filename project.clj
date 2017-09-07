;;;
;;; Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
;;;
;;;This source code is licensed under the terms described in the associated LICENSE.TXT file.
;;;
(defproject ohua-jvm-runtime "0.1-SNAPSHOT"
  :description "A runtime for Ohua on the JVM."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo" :creds :gpg}]]

  :plugins [[lein-pprint "1.1.1"]
            ; plugin needed for META-INF handling
            [lein-javac-resources "0.1.1"]]

  :dependencies [; code generation
                 [org.codehaus.janino/janino "3.0.6"]]

  :profiles {:provided {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :dev      {:plugins                 [[lein-junit "1.1.8"]
                                                  ;[lein-scalac "0.1.0"] Doesn't work, I'm using manually compiled scala code right now
                                                  ]
                        :dependencies            [
                                                  ; libs needed for testing
                                                  [junit/junit "4.12"]

                                                  [org.scala-lang/scala-library "2.11.8"] ; Necessary to use scala stdlib
                                                  [org.scala-lang/scala-compiler "2.10.1"] ; I'm not actually sure its necessary, I think I'm just using my
                                                                                           ; systems scala compiler right now but I left it in here as a reminder
                                                  [rhizome "0.2.5"]
                                                  ]

                        ; paths for Clojure test cases
                        ;:test-paths              ["test/clojure"]

                        ; paths for Java/JUnit test cases
                        :junit                   ["test/java"]
                        ;:junit-test-file-pattern #".*multithreading\/test[0-1a-zA-Z]*\.java"
                        :junit-test-file-pattern #".*\/test[0-1a-zA-Z]*\.java"
                        :junit-formatter         :brief
                        ; we skip the below option to print the output to the command line
                        ;:junit-results-dir       "test-output-lein"

                        :java-source-paths       ["test/java"]
                        ;:scala-source-path       "com.ohua.lang/test/scala" Source path definition for the leinigen plugin, which is broken
                        :jvm-opts                ["-ea"]
                        ;:prep-tasks ["scalac"] Doesn't work -.-
                        }}


  ;:source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :jvm-opts [;"-XX:-ProfileInterpreter"
             ;"-XX:-TieredCompilation"
             ;"-XX:Tier3InvocationThreshold=1000"
             ;"-XX:ParallelGCThreads=2"
             "-Xmx2g"] ; there is no need for more than that during development (some tests run on quite a lot of data)

  ; I'm not sure anymore whether it is a good idea to rely on lein-junit further as it depends on lancet which is dead.
  :junit-options {:fork "on"
                  ;:forkMode "perTest"
                  }

  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options" "-g"]

  ; this is needed to handle META-INF directories properly (copy them to :compile-path)
  :hooks [leiningen.javac-resources]
  ;:omit-source true  ; avoids .java files ending up in the generated JAR file
  ; with the extension above we must explicitly exclude java source files!
  ; we can't use the :omit-source option because it also looses the Clojure sources.
  ; I filed a bug on this: https://github.com/kumarshantanu/lein-javac-resources/issues/1
  :jar-exclusions [#"\.java$"]
  )
