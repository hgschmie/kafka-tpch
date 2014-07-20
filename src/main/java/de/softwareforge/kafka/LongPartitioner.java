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

import com.google.common.primitives.Ints;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class LongPartitioner
        implements Partitioner
{
    public LongPartitioner(VerifiableProperties props)
    {
    }

    @Override
    public int partition(Object value, int numPartitions)
    {
        if (value instanceof Long) {
            return Ints.checkedCast(((Long) value) % numPartitions);
        }
        else {
            return 0;
        }
    }
}
