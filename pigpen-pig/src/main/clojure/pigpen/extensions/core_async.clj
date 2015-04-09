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

(ns pigpen.extensions.core-async
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols])
  (:import [clojure.core.async.impl.protocols Channel]))

(set! *warn-on-reflection* true)

(defrecord ChannelException [z])

(defn channel? [ch]
  (instance? Channel ch))

(defmacro safe->!
  "A safe version of clojure.core.async/>!

Use with safe-<! and safe-<!!

  * Converts nils into a channel-safe keyword.
  * Catches any exceptions thrown by the evaluation of val and packages them
into the channel.
"
  [chan val]
  `(try
     (if-let [val# ~val]
       (a/>! ~chan val#)
       (a/>! ~chan ::nil))
     (catch Throwable z#
       (a/>! ~chan (->ChannelException z#)))))

(defmacro safe->!!
  "A safe version of clojure.core.async/>!!

Use with safe-<! and safe-<!!

  * Converts nils into a channel-safe keyword.
  * Catches any exceptions thrown by the evaluation of val and packages them
into the channel.
"
  [chan val]
  `(try
     (if-let [val# ~val]
       (do (a/>!! ~chan val#))
       (do (a/>!! ~chan ::nil)))
     (catch Throwable z#
       (a/>!! ~chan (ChannelException. z#)))))

(defmacro safe-<!
  "A safe version of clojure.core.async/<!

Use with safe->!, safe->!!, and safe-go

  * Converts the channel-safe nil keyword into an actual nil
  * Re-throws packaged exceptions
  * Converts nils into a unique completion keyword
"
  [chan]
  `(let [val# (a/<! ~chan)]
     (cond
       (= val# ::nil) nil
       (instance? ChannelException val#) (throw (:z val#))
       (nil? val#) ::complete
       :else val#)))

(defmacro safe-<!!
  "A safe version of clojure.core.async/<!!

Use with safe->!, safe->!!, and safe-go

  * Converts the channel-safe nil keyword into an actual nil
  * Re-throws packaged exceptions
  * Converts nils into a unique completion keyword
"
  [chan]
  `(let [val# (a/<!! ~chan)]
     (cond
       (= val# ::nil) nil
       (instance? ChannelException val#) (throw (:z val#))
       (nil? val#) ::complete
       :else val#)))

(defmacro safe-go
  "A safe version of clojure.core.async/go

Use with safe->! and safe-<!

  * Converts nils into a channel-safe keyword.
  * Catches any exceptions thrown by the evaluation of body and packages them
into the channel.
"
  [& body]
  `(a/go
     (try
       (if-let [result# (do ~@body)]
         result#
         ::nil)
     (catch Throwable z# (ChannelException. z#)))))

(defn chan->lazy-seq
  "Pulls values from a channel until the channel is closed.

Use with safe->!, safe->!!, and safe-go
"
  [ch]
  (take-while #(not= ::complete %) (repeatedly #(safe-<!! ch))))
