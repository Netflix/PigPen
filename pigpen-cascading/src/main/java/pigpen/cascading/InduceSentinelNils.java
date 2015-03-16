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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;

public class InduceSentinelNils extends BaseOperation implements Function {

    private static final IFn INDUCE_NILS = OperationUtil.getVar("induce-sentinel-nil");

    private final Integer index;

    public InduceSentinelNils(final Integer index, final Fields fields) {
        super(fields);
        this.index = index;
    }

    @Override
    public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {
        INDUCE_NILS.invoke(functionCall, this.index);
    }
}
