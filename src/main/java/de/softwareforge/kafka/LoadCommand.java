/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.softwareforge.kafka;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.command.Command;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.log.LoggingMBean;
import io.airlift.tpch.TpchTable;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;

@Command(name = "load", description = "Load TPCH data into Kafka")
public class LoadCommand
        extends TpchMain.TpchCommand
{
    private static final Logger LOG = Logger.get(LoadCommand.class);

    private final ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();

    @Inject
    public LoaderOptions loaderOptions = new LoaderOptions();

    @Override
    public void execute()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.configure(new LoggingConfiguration());
        new LoggingMBean().setLevel("kafka", "ERROR");

        String tableNames = loaderOptions.tables;
        Map<String, TpchTable<?>> allTables = ImmutableMap.copyOf(Maps.uniqueIndex(TpchTable.getTables(), new Function<TpchTable<?>, String>()
        {
            @Override
            public String apply(@Nonnull TpchTable<?> input)
            {
                return input.getTableName();
            }
        }));

        List<String> tables;
        if (tableNames == null) {
            tables = ImmutableList.copyOf(allTables.keySet());
        }
        else {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (String tableName : Splitter.on(",").omitEmptyStrings().trimResults().split(tableNames)) {
                checkState(allTables.keySet().contains(tableName), "Table %s is unknown", tableName);
                builder.add(tableName);
            }
            tables = builder.build();
        }

        LOG.info("Processing tables: %s", tables);

        Properties props = new Properties();
        props.put("zk.connect", loaderOptions.zookeeper);
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", LongPartitioner.class.getName());
        props.put("serializer.encoding", "UTF8");
        props.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(props);

        ObjectMapper mapper = objectMapperProvider.get();
        mapper.enable(MapperFeature.AUTO_DETECT_GETTERS);

        Producer producer = new Producer<String, String>(producerConfig);

        for (String table : tables) {
            TpchTable<?> tpchTable = allTables.get(table);
            LOG.info("Loading table '%s' into topic '%s%s'...", table, loaderOptions.prefix, table);
            long count = 0;
            for (Object o : tpchTable.createGenerator(loaderOptions.tpchType.getScaleFactor(), 1, 1)) {
                String value = mapper.writeValueAsString(o);
                ProducerData message = new ProducerData<String, String>(loaderOptions.prefix + table, value);
                producer.send(message);
                count++;
            }
            LOG.info("Generated %d rows for table '%s'.", count, table);
        }

        producer.close();
    }
}
