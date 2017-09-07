(ns ohua.util.visual
  (:require [rhizome.dot :refer [graph->dot]]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.string :refer [join]])
  (:import  (ohua.runtime.engine.flowgraph.elements.operator WorkBasedOperatorRuntime OperatorCore OutputPort Arc InputPort)))


(def ir-graphs-directory "ir-graphs")


(defn find [thing coll]
  (first (filter #(= thing %) coll)))


(defn- op-core->map [^OperatorCore op]
  {:name (.getOperatorName op)
   :id   (.getIDInt (.getId op))})


(defn arc->map [^Arc a]
  {:out-port (.getIDInt (.getPortId (.getSourcePort a)))
   :in-port  (.getIDInt (.getPortId (.getTargetPort a)))
   :target   (op-core->map (.getTarget a))
   :boundary (.getArcBoundary a)
   :load     (.getLoadEstimate a)})


(defn has-capacity? [{load :load boundary :boundary}]
  (> boundary (* 2 load)))


(defn get-op-status [{succs :successors in-ports :input-ports}]
  (let [has-input (every? (fn [^InputPort p]
                            (not (.isQueueEmpty (.getIncomingArc p))))
                          in-ports)
        can-output (every? has-capacity? (apply concat (vals succs)))]
    (if has-input
      (if can-output
        :ready
        :blocked)
      :no-input)))


(defn render-op-graph
  ([gr filename] (render-op-graph gr nil filename))
  ([gr chosen filename]
   (let [; [ {:operator {:name String, :id int}
         ;    :successors [{:port int, :target {:name String, :id int}}]} ]
         conn-map (into {}
                        (map
                          (fn [^OperatorCore op]
                            (let [{opid :id :as op-m} (op-core->map op)]
                              [opid
                               {:operator    op-m
                                :input-ports (into [] (.getInputPorts op))
                                :successors  (group-by
                                               (comp :id :target)
                                               (mapcat
                                                 (fn [^OutputPort p]
                                                   (map
                                                     arc->map
                                                     (.getOutgoingArcs p)))
                                                 (.getOutputPorts op)))
                                }]))
                          (into [] gr)))

         ]
     (spit filename
           (graph->dot
             (keys conn-map)
             (comp keys :successors conn-map)
             :node->descriptor
             #(let [{{name :name id :id} :operator :as connection} (conn-map %)]
                {:label (str name "<" id ">")
                 :shape "box"
                 :color (if (and (not (nil? chosen)) (= chosen id))
                          "blue"
                          (case (get-op-status connection)
                            :blocked "red"
                            :ready "green"
                            :no-input "black"))})
             :edge->descriptor
             (fn [source-id target-id]
               (let [{succ-map :successors} (conn-map source-id) ; should be aggrgate or average but I'm lazy
                     [{ip :in-port op :out-port load :load boundary :boundary :as arc}] (succ-map target-id)]
                 {:label (str op "->" ip " (" load "/" boundary ")")
                  :color (if (has-capacity? arc)
                           "black"
                           "red")})))
           ))))


(defn render-runtime-graph
  ([gr filename] (render-runtime-graph gr nil filename))
  ([gr chosen filename] (render-op-graph (map #(.getOp %) gr) chosen filename)))


(defn graph-to-str [fns]
  (str
    "(let ["
    (join "\n      "
          (map
            (fn [{name :name args :args return :return id :id}]
              (str
                (if (symbol? return) return (into [] return))
                " (" (join " " (cons (str name "<" id ">") args)) ")"))
            fns))
    "]\n  "
    (:return (last fns))
    ")"))
