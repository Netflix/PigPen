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

(ns pigpen.rx.extensions.core-test
  (:use clojure.test
        pigpen.rx.extensions.core)
  (:import [rx Observable Observer Subscription]
           [rx.observables BlockingObservable]))

(deftest test-multicast
  
  ;; TODO relax the timing here. Sometimes this will show a false negative
  (let [results (vec (repeatedly 7 #(atom []))) 
        track (fn [a] (fn [n] (swap! (results a) conj n)))

        values (atom [])

        data (Observable/create
               (fn [^Observer o]
                 (let [cancel (atom false)]
                   (future
                     (doseq [i (take-while (fn [_] (not @cancel)) (range))]
                       (.onNext o i)
                       (swap! values #(conj % i))
                       (Thread/sleep 3)))
                   (reify Subscription
                     (unsubscribe [this]
                       (swap! cancel (constantly true)))))))
        mo (multicast data)

        ;; create two children
        o1 (mo)
        o2 (mo)
        ;; first subscription on o1, don't start
        _ (-> o1 (.take 1) (.subscribe (track 0)))
        _ (Thread/sleep 10)
        ;; second subscription on o1, don't start
        _ (-> o1 (.take 2) (.subscribe (track 1)))
        _ (Thread/sleep 10)
        ;; first subscription on o2, start
        _ (-> o2 (.take 3) (.subscribe (track 2)))
        ;; it should finish by now
        _ (Thread/sleep 10)
        ;; subscribe to o2 again
        _ (-> o2 (.take 4) (.subscribe (track 3)))
        ;; create a third child & subscribe
        o3 (mo)
        _ (-> o3 (.take 4) (.subscribe (track 4)))
        ;; subscribe to o1, start over
        _ (-> o1 (.take 4) (.subscribe (track 5)))
        ;; wait until halfway done
        _ (Thread/sleep 5)
        ;; add another child
        o4 (mo)
        _ (-> o4 (.take 4) (.subscribe (track 6)))
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
  
  (let [d0 (Observable/from [1 2 3])
        d1 (Observable/from [4 5 6])
        m0 (multicast d0)
        m1 (multicast d1)
        j0 (Observable/merge [(m0) (m1)])
        j1 (Observable/merge [(m0) (m1)])
        s0 (Observable/merge [j0 j1])
        v0 (BlockingObservable/toIterable s0)]
    (is (= (sort (seq v0)) [1 1 2 2 3 3 4 4 5 5 6 6]))))
