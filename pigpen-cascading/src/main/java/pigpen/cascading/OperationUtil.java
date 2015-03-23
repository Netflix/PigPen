/*
 *
 *  Copyright 2015 Netflix, Inc.
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

package pigpen.cascading;

import org.apache.hadoop.io.BytesWritable;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class OperationUtil {

    public static IFn getVar(final String name) {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.runtime"));
        require.invoke(Symbol.intern("pigpen.cascading.runtime"));
        return RT.var("pigpen.cascading.runtime", name);
    }

    public static byte[] getBytes(final BytesWritable bw) {
        if (bw.getCapacity() == bw.getLength()) {
            return bw.getBytes();
        } else {
            return copyBytes(bw);
        }
    }

    public static byte[] copyBytes(final BytesWritable bw) {
        final byte[] ret = new byte[bw.getLength()];
        System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
        return ret;
    }
}
