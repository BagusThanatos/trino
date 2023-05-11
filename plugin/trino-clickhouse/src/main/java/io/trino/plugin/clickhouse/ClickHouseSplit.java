/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

public class ClickHouseSplit extends JdbcSplit {
    private final Optional<String> tableName;

    public ClickHouseSplit(
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("tableName") Optional<String> tableName) {
        this(additionalPredicate, tableName, TupleDomain.all());
    }

    @JsonCreator
    public ClickHouseSplit(
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("tableName") Optional<String> tableName,
            @JsonProperty("dynamicFilter") TupleDomain<JdbcColumnHandle> dynamicFilter) {
        super(additionalPredicate, dynamicFilter);
        this.tableName = tableName;
    }

    @JsonProperty
    public Optional<String> getTableName() {
        return this.tableName;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public JdbcSplit withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return new ClickHouseSplit(getAdditionalPredicate(), tableName, dynamicFilter);
    }
}
