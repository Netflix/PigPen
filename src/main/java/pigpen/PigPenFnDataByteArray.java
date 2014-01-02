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

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * A user function that returns a DataByteArray.
 *
 * @author mbossenbroek
 *
 */
public class PigPenFnDataByteArray extends AccumulatorEvalFunc<DataByteArray> {

    private Object state = null;

    @Override
    public void accumulate(Tuple input) throws IOException {
        state = ClojureForPigs.accumulate(state, input);
    }

    @Override
    public DataByteArray getValue() {
        return (DataByteArray) ClojureForPigs.getValue(state);
    }

    @Override
    public void cleanup() {
        state = ClojureForPigs.cleanup(state);
    }
}
