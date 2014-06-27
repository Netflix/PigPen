;;
;;
;;  Copyright 2013 Netflix, Inc.
;;
;;     Licensed under the Apache License, Version 2.0 (the "License");
;;     you may not use this file except in compliance with the License.
;;     You may obtain a copy of the License at
;;
;;         http://www.apache.org/licenses/LICENSE-2.0
;;
;;     Unless required by applicable law or agreed to in writing, software
;;     distributed under the License is distributed on an "AS IS" BASIS,
;;     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;     See the License for the specific language governing permissions and
;;     limitations under the License.
;;
;;

(ns pigpen.rx.extensions.core
  (:require [clojure.set :as set])
  (:import [rx Observable Observer Subscription]))

(set! *warn-on-reflection* true)

(defn ^:private obj->id [obj]
  (second (re-find #"@([0-9a-f]+)" (str obj))))

(defn ^:private observers->ids [o]
  (->> o
    (map (fn [[k os]] [k (map obj->id os)]))
    (into {})))

(defn multicast
  "Takes an observable and returns a multicast observable. To use the return
   value, invoke it and it will supply a new observable. Once all child
   observables have been subscribed to, it will automatically start the
   subscription to the parent. Once all children have unsubscribed, it will
   automatically unsubscribe from the parent. Children can have multiple
   subscriptions & they will not create multiple subscriptions to the parent.
   However, as soon as the last child is subscribed to, the parent subscription
   is started."
  ([^Observable parent] (multicast parent nil))
  ([^Observable parent debug]
    (let [children (atom {:observables #{}
                          :observers {}
                          :subscription nil
                          :current nil})
        
          add-observable (fn [c id o] (-> c
                                        (update-in [:observables] #((fnil conj #{}) % id))
                                        (assoc :current o)))
        
          all-observers #(->> % :observers (vals) (apply concat))
          push-observers #(doseq [observer (all-observers @children)] (% observer))
          add-observer (fn [c id o] (update-in c [:observers id] #((fnil conj #{}) % o)))
          remove-observer (fn [c id o u]
                            (let [c (update-in c [:observers id] #(disj % o))]
                              (if (not-empty (all-observers c)) c
                                (update-in c [:subscription] (partial u id)))))

          subscribe (fn [id]
                      (if debug (println debug "subscribe" id (obj->id parent)))
                      (.subscribe parent
                        (fn [next] (push-observers (fn [^Observer o] (.onNext o next))))
                        (fn [error] (push-observers (fn [^Observer o] (.onError o error))))
                        (fn [] (push-observers (fn [^Observer o] (.onCompleted o))))))
          unsubscribe (fn [id ^Subscription s]
                        (if debug (println debug "unsubscribe" id (obj->id parent)))
                        (if s (.unsubscribe s)))
        
          observable (fn [id]
                       (if debug (println debug "observable" id))
                       (Observable/create
                         (fn [^Observer o]
                           (let [{:keys [observables observers]} (swap! children add-observer id o)]
                             (if debug (println debug "observer" id observables (observers->ids observers)))
                             (when (empty? (set/difference observables (->> observers (filter (comp not-empty val)) (keys))))
                               (locking children
                                 (when-not (:subscription @children)
                                   (swap! children assoc :subscription (subscribe id))))))
                           (reify Subscription
                             (unsubscribe [this]
                               (swap! children remove-observer id o unsubscribe))))))]
      (fn []
        (let [id (gensym)]
          (:current (swap! children add-observable id (observable id))))))))
