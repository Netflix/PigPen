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

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

/**
 * Used to execute Clojure code from within a Pig UDF. This implements the Algebraic interface.
 *
 * @author mbossenbroek
 *
 */
public class PigPenFnAlgebraic extends EvalFunc<DataByteArray> implements Algebraic {

    private static final IFn EVAL_STRING, ALGEBRAIC;
    private static final Keyword EXEC = RT.keyword(null, "exec");
    private static final Keyword INITIAL = RT.keyword(null, "initial");
    private static final Keyword INTERMED = RT.keyword(null, "intermed");
    private static final Keyword FINAL = RT.keyword(null, "final");

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.pig.runtime"));
        EVAL_STRING = RT.var("pigpen.pig.runtime", "eval-string");
        ALGEBRAIC = RT.var("pigpen.pig.runtime", "udf-algebraic");
    }

    private final String initString, funcString;
    private final Object func;

    public PigPenFnAlgebraic(String init, String func) {
        this.initString = init;
        this.funcString = func;
        EVAL_STRING.invoke(init);
        this.func = EVAL_STRING.invoke(func);
    }

    @Override
    public DataByteArray exec(final Tuple input) throws IOException {
        return (DataByteArray) ALGEBRAIC.invoke(func, EXEC, input);
    }

    @Override
    public String getInitial() {
        return Initial.class.getName() + "('" + initString + "','" + funcString + "')";
    }

    @Override
    public String getIntermed() {
        return Intermed.class.getName() + "('" + initString + "','" + funcString + "')";
    }

    @Override
    public String getFinal() {
        return Final.class.getName() + "('" + initString + "','" + funcString + "')";
    }

    /**
     * The Initial phase.
     */
    public static class Initial extends EvalFunc<Tuple> {

        private final Object func;

        public Initial(String init, String func) {
            // there are no words to describe how I feel about this crap
            if (!init.equals("null")) {
                EVAL_STRING.invoke(init);
                this.func = EVAL_STRING.invoke(func);
            } else {
                this.func = null;
            }
        }

        @Override
        public Tuple exec(final Tuple input) throws IOException {
            return (Tuple) ALGEBRAIC.invoke(func, INITIAL, input);
        }
    }

    /**
     * The Intermed phase.
     */
    public static class Intermed extends EvalFunc<Tuple> {

        private final Object func;

        public Intermed(String init, String func) {
            if (!init.equals("null")) {
                EVAL_STRING.invoke(init);
                this.func = EVAL_STRING.invoke(func);
            } else {
                this.func = null;
            }
        }

        @Override
        public Tuple exec(final Tuple input) throws IOException {
            return (Tuple) ALGEBRAIC.invoke(func, INTERMED, input);
        }
    }

    /**
     * The Final phase.
     */
    public static class Final extends EvalFunc<DataByteArray> {

        private final Object func;

        public Final(String init, String func) {
            if (!init.equals("null")) {
                EVAL_STRING.invoke(init);
                this.func = EVAL_STRING.invoke(func);
            } else {
                this.func = null;
            }
        }

        @Override
        public DataByteArray exec(final Tuple input) throws IOException {
            return (DataByteArray) ALGEBRAIC.invoke(func, FINAL, input);
        }
    }
}
