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

package pigpen.parquet;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class PigPenParquetWriteSupport extends WriteSupport<Map<String, Object>> {

    private static final IFn WRITE;

    static {
        final Var require = RT.var("clojure.core", "require");
        require.invoke(Symbol.intern("pigpen.parquet.core"));
        WRITE = RT.var("pigpen.parquet.core", "write");
    }

    private RecordConsumer recordConsumer;
    private MessageType rootSchema;

    @Override
    public WriteSupport.WriteContext init(final Configuration configuration) {
        final String schema = configuration.get("schema");
        this.rootSchema = MessageTypeParser.parseMessageType(schema);
        return new WriteContext(this.rootSchema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(final Map<String, Object> record) {
        WRITE.invoke(this.recordConsumer, this.rootSchema, record);
    }
}
