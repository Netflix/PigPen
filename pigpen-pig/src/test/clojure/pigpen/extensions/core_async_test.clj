;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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

(ns pigpen.extensions.core-async-test
  (:use clojure.test
        pigpen.extensions.core-async)
  (:require [clojure.core.async :as a]))

(deftest test-channel?
  (let [c (a/chan)]
    (is (channel? c))
    (is (not (channel? 42)))))

(deftest test-safe->!
  
  (testing "single"
    (let [c (a/chan)]
      (a/go (safe->! c 1))
      (is (= (a/<!! (a/go (safe-<! c))) 1))))
  
  (testing "many"
    (let [c (a/chan)]
      (a/go
        (safe->! c 1)
        (safe->! c 2)
        (safe->! c 3))
      (is (= [1 2 3]
             (a/<!!
               (a/go
                 [(safe-<! c)
                  (safe-<! c)
                  (safe-<! c)]))))))
  
  (testing "nil"
    (let [c (a/chan)]
      (a/go
        (safe->! c 1)
        (safe->! c nil)
        (safe->! c 3))
      (is (= [1 nil 3]
             (a/<!!
               (a/go
                 [(safe-<! c)
                  (safe-<! c)
                  (safe-<! c)]))))))
  
  (testing "exception"
    (let [c (a/chan 10)]
      (a/go
        (safe->! c 1)
        (safe->! c (throw (Exception.)))
        (safe->! c 3))
      (let [[x y z] (a/<!!
                       (a/go
                         [(try (safe-<! c) (catch Throwable z z))
                          (try (safe-<! c) (catch Throwable z z))
                          (try (safe-<! c) (catch Throwable z z))]))]
        (is (= x 1))
        (is (instance? Exception y))
        (is (= z 3))))))

(deftest test-safe->!!
  
  (testing "single"
    (let [c (a/chan 10)]
      (safe->!! c 1)
      (is (= (safe-<!! c) 1))))
  
  (testing "many"
     (let [c (a/chan 10)]
       (safe->!! c 1)
       (safe->!! c 2)
       (safe->!! c 3)
       (is (= (safe-<!! c) 1))
       (is (= (safe-<!! c) 2))
       (is (= (safe-<!! c) 3))))
  
  (testing "nil"
    (let [c (a/chan 10)]
      (safe->!! c 1)
      (safe->!! c nil)
      (safe->!! c 3)
      (is (= (safe-<!! c) 1))
      (is (= (safe-<!! c) nil))
      (is (= (safe-<!! c) 3))))
  
  (testing "exception"
    (let [c (a/chan 10)]
      (safe->!! c 1)
      (safe->!! c (throw (Exception.)))
      (safe->!! c 3)
      (is (= (safe-<!! c) 1))
      (is (thrown? Exception (safe-<!! c)))
      (is (= (safe-<!! c) 3)))))

(deftest test-safe-go
  
  (testing "normal"
    (let [c (a/chan)]
      (a/go (safe->! c 1))
      (is (= (safe-<!! (safe-go (safe-<! c))) 1))))
  
  (testing "nil"
    (is (nil? (safe-<!! (safe-go nil)))))
  
  (testing "exception"
    (let [c (a/chan 10)]
      (a/go
        (safe->! c 1)
        (safe->! c (throw (Exception.)))
        (safe->! c 3))
      
      (is (= (safe-<!! (safe-go (safe-<! c))) 1))
      (is (thrown? Exception (safe-<!! (safe-go (safe-<! c)))))
      (is (= (safe-<!! (safe-go (safe-<! c))) 3)))))

(deftest test-chan->lazy-seq
  
  (testing "normal"
    (let [c (a/chan 10)]
      (a/go (safe->! c 1)
            (safe->! c 2)
            (safe->! c 3)
            (a/close! c))
      (is (= (chan->lazy-seq c)
             [1 2 3]))))
  
  (testing "with nil"
    (let [c (a/chan 10)]
      (a/go (safe->! c 1)
            (safe->! c nil)
            (safe->! c 3)
            (a/close! c))
      (is (= (chan->lazy-seq c)
             [1 nil 3])))))
