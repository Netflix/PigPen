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

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

/**
 * Used to execute Clojure code from within a Pig UDF. Passes the tuple directly to pigpen.pig/eval-udf
 *
 * @param <T> The return type
 *
 * @author mbossenbroek
 *
 */
public class PigPenFn<T> extends EvalFunc<T> implements Accumulator<T> {

    private static final IFn EVAL, ACCUMULATE, GET_VALUE, CLEANUP;

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.pig"));
        EVAL = RT.var("pigpen.pig", "eval-udf");
        ACCUMULATE = RT.var("pigpen.pig", "udf-accumulate");
        GET_VALUE = RT.var("pigpen.pig", "udf-get-value");
        CLEANUP = RT.var("pigpen.pig", "udf-cleanup");
    }

    protected final String init, func;

    public PigPenFn(String init, String func) {
        this.init = init;
        this.func = func;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T exec(Tuple input) throws IOException {
        return (T) EVAL.invoke(init, func, input);
    }

    private Object state = null;

    @Override
    public void accumulate(Tuple input) throws IOException {
        state = ACCUMULATE.invoke(init, func, state, input);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getValue() {
        return (T) GET_VALUE.invoke(state);
    }

    @Override
    public void cleanup() {
        state = CLEANUP.invoke(state);
    }
}
