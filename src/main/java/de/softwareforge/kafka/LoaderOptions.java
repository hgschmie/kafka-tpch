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

import io.airlift.command.Option;

public class LoaderOptions
{
    @Option(name = "--brokers", required = true, title = "brokers", description = "One or more Kafka brokers (host:port), separated by comma")
    public String brokers = null;

    @Option(name = "--tpch-type", title = "tpch-type", description = "TPCH type to load (supported are tiny, sf1, sf100, sf300, sf1000, sf3000, sf10000 sf30000 and sf100000)")
    public TpchType tpchType = TpchType.tiny;

    @Option(name = "--tables", title = "tables", description = "Tables to load. If omitted, all available tables are loaded.")
    public String tables = null;

    @Option(name = "--prefix", title = "prefix", description = "Table name prefix.")
    public String prefix = "";

    public enum TpchType
    {
        tiny(0.01),
        sf1(1),
        sf100(100),
        sf300(300),
        sf1000(1000),
        sf3000(3000),
        sf10000(10000),
        sf30000(30000),
        sf100000(100000);

        private final double scaleFactor;

        private TpchType(double scaleFactor)
        {
            this.scaleFactor = scaleFactor;
        }

        public double getScaleFactor()
        {
            return scaleFactor;
        }
    }
}

