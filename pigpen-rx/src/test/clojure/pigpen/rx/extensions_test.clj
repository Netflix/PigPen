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

(ns pigpen.rx.extensions-test
  (:use clojure.test
        pigpen.rx.extensions)
  (:require [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rx-block])
  (:import [rx Observable Observer Subscriber Subscription]
           [rx.observables BlockingObservable]))

(deftest test-multicast

  ;; TODO relax the timing here. Sometimes this will show a false negative
  (let [results (vec (repeatedly 7 #(atom [])))
        track (fn [a] (fn [n] (swap! (results a) conj n)))

        values (atom [])

        data (rx/observable*
               (fn [^Subscriber o]

                 (let [cancel (atom false)]
                   (.add o
                         (reify Subscription
                           (unsubscribe [this]
                             (swap! cancel (constantly true)))))
                   (future
                     (doseq [i (take-while (fn [_] (not @cancel)) (range))]
                       (.onNext o i)
                       (swap! values #(conj % i))
                       (Thread/sleep 3))))))
        mo (multicast data)

        ;; create two children
        o1 (mo)
        o2 (mo)
        ;; first subscription on o1, don't start
        _ (rx/subscribe (rx/take 1 o1) (track 0))
        _ (Thread/sleep 10)
        ;; second subscription on o1, don't start
        _ (rx/subscribe (rx/take 2 o1) (track 1))
        _ (Thread/sleep 10)
        ;; first subscription on o2, start
        _ (rx/subscribe (rx/take 3 o2) (track 2))
        ;; it should finish by now
        _ (Thread/sleep 10)
        ;; subscribe to o2 again
        _ (rx/subscribe (rx/take 4 o2) (track 3))
        ;; create a third child & subscribe
        o3 (mo)
        _ (rx/subscribe (rx/take 4 o3) (track 4))
        ;; subscribe to o1, start over
        _ (rx/subscribe (rx/take 4 o1) (track 5))
        ;; wait until halfway done
        _ (Thread/sleep 5)
        ;; add another child
        o4 (mo)
        _ (rx/subscribe (rx/take 4 o4) (track 6))
        _ (Thread/sleep 50)]
    (is (= [0 1 2 0 1 2 3 4 5] @values))
    (is (= (map deref results)
           [[0]
            [0 1]
            [0 1 2]
            [0 1 2 3]
            [0 1 2 3]
            [0 1 2 3]
            [2 3 4 5]]))))

(deftest test-multicast-crossover

  (let [d0 (rx/seq->o [1 2 3])
        d1 (rx/seq->o [4 5 6])
        m0 (multicast d0)
        m1 (multicast d1)
        j0 (rx/merge (m0) (m1))
        j1 (rx/merge (m0) (m1))
        s0 (rx/merge j0 j1)
        v0 (rx-block/into [] s0)]
    (is (= (sort v0)
           [1 1 2 2 3 3 4 4 5 5 6 6]))))
