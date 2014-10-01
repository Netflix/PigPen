(ns pigpen.pig.raw)

(defn register$
  "A Pig REGISTER command. jar is the qualified location of the jar."
  [jar]
  {:pre [(string? jar)]}
  ^:pig {:type :register
         :jar jar})

(defn option$
  "A Pig option. Takes the name and a value. Not used locally."
  [option value]
  {:pre [(string? option)]}
  ^:pig {:type :option
         :option option
         :value value})
