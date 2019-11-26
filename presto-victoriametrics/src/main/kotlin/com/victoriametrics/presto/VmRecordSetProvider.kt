/**
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
package com.victoriametrics.presto

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.ConnectorSession
import com.facebook.presto.spi.ConnectorSplit
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSplit
import okhttp3.OkHttpClient
import javax.inject.Inject

class VmRecordSetProvider
@Inject constructor(
        private val httpClient: OkHttpClient,
        private val queryBuilder: QueryBuilder,
        private val metadata: VmMetadata
) : ConnectorRecordSetProvider {

    override fun getRecordSet(
            transactionHandle: ConnectorTransactionHandle,
            session: ConnectorSession,
            split: ConnectorSplit,
            columns: List<ColumnHandle>
    ): VmRecordSet {
        split as VmSplit
        val vmColumns = columns.map { it as VmColumnHandle }
        val allColumnsByName = metadata.columns
                        .map { it.name to it }
                        .toMap()

        return VmRecordSet(
                split.constraint,
                queryBuilder,
                httpClient,
                vmColumns,
                allColumnsByName
        )
    }
}
