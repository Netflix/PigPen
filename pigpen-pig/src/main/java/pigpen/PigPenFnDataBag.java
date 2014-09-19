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
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * A user function that returns a DataBag.
 *
 * @author mbossenbroek
 *
 */
public class PigPenFnDataBag extends PigPenFn<DataBag> {

    public PigPenFnDataBag(String init, String func) {
        super(init, func);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        return BagFactory.getInstance().newDefaultBag((List<Tuple>) EVAL.invoke(func, input));
    }
}
