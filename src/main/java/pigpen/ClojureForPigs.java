/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package pigpen;

import java.io.IOException;

import org.apache.pig.data.Tuple;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

/**
 * Used to execute Clojure code from within a Pig UDF. Passes the tuple directly to pigpen.pig/eval-udf
 *
 * @author mbossenbroek
 *
 */
public final class ClojureForPigs {

    private static final IFn EVAL, ACCUMULATE, GET_VALUE, CLEANUP;

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.pig"));
        EVAL = RT.var("pigpen.pig", "eval-udf");
        ACCUMULATE = RT.var("pigpen.pig", "udf-accumulate");
        GET_VALUE = RT.var("pigpen.pig", "udf-get-value");
        CLEANUP = RT.var("pigpen.pig", "udf-cleanup");
    }

    /**
     * Invokes the Clojure code specified by the tuple.
     *
     * @param tuple
     *            The tuple passed to the Pig UDF
     * @return The result
     * @throws IOException
     */
    public static Object invoke(Tuple tuple) throws IOException {
        return EVAL.invoke(tuple);
    }

    /**
     * Invokes the Clojure code specified by the tuple.
     *
     * @param state
     *            The existing state
     * @param value
     *            The tuple passed to the Pig UDF
     * @return The new state
     * @throws IOException
     */
    public static Object accumulate(Object state, Tuple value) throws IOException {
        return ACCUMULATE.invoke(state, value);
    }

    /**
     * Invokes the Clojure code specified by the tuple.
     *
     * @param state
     *            The existing state
     * @return The result
     */
    public static Object getValue(Object state) {
        return GET_VALUE.invoke(state);
    }

    /**
     * Invokes the Clojure code specified by the tuple.
     *s
     * @param state
     *            The existing state
     * @return The new state
     */
    public static Object cleanup(Object state) {
        return CLEANUP.invoke(state);
    }

    private ClojureForPigs() {
    }
}
