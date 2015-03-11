(ns pigpen.query
  (:require [pigpen.raw :as raw]))

(defn store-many
  "Combines multiple store commands into a single script. This is not required
if you have a single output.

  Example:

    (pig/store-many
      (pig/store-tsv \"foo.tsv\" foo)
      (pig/store-clj \"bar.clj\" bar))

  Note: When run locally, this will merge the results of any source relations.
"
  {:arglists '([outputs+])
   :added "0.1.0"}
  [& outputs]
  (raw/store-many$ outputs))
