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
package io.trino.plugin.deltalake.transactionlog;

import io.trino.filesystem.Location;

import static io.trino.filesystem.Locations.appendPath;

public final class TransactionLogUtil
{
    private TransactionLogUtil() {}

    public static final String TRANSACTION_LOG_DIRECTORY = "_delta_log";

    public static String getTransactionLogDir(String tableLocation)
    {
        return appendPath(tableLocation, TRANSACTION_LOG_DIRECTORY);
    }

    public static Location getTransactionLogJsonEntryPath(String transactionLogDir, long entryNumber)
    {
        return Location.of(transactionLogDir).appendPath("%020d.json".formatted(entryNumber));
    }
}
