/*
 *
 *  Copyright 2013-2015 Netflix, Inc.
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
import org.apache.pig.data.DataBag;
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
public class PigPenFn extends EvalFunc<DataBag> implements Accumulator<DataBag> {

    protected static final IFn EVAL_STRING, EXEC, EVAL, ACCUMULATE, GET_VALUE, CLEANUP;

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.pig.runtime"));
        EVAL_STRING = RT.var("pigpen.pig.runtime", "eval-string");
        EXEC = RT.var("pigpen.pig.runtime", "exec-transducer");
        EVAL = RT.var("pigpen.pig.runtime", "eval-udf");
        ACCUMULATE = RT.var("pigpen.pig.runtime", "udf-accumulate");
        GET_VALUE = RT.var("pigpen.pig.runtime", "udf-get-value");
        CLEANUP = RT.var("pigpen.pig.runtime", "udf-cleanup");
    }

    protected final Object func;

    public PigPenFn(String init, String func) {
        EVAL_STRING.invoke(init);
        this.func = EXEC.invoke(EVAL_STRING.invoke(func));
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        return (DataBag) EVAL.invoke(func, input);
    }

    private Object state = null;

    @Override
    public void accumulate(Tuple input) throws IOException {
        state = ACCUMULATE.invoke(func, state, input);
    }

    @Override
    public DataBag getValue() {
        return (DataBag) GET_VALUE.invoke(state);
    }

    @Override
    public void cleanup() {
        state = CLEANUP.invoke(state);
    }
}
