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
import com.facebook.presto.spi.ConnectorTableLayoutHandle
import com.facebook.presto.spi.FixedSplitSource
import com.facebook.presto.spi.connector.ConnectorSplitManager
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.predicate.TupleDomain
import com.victoriametrics.presto.model.VmConfig
import com.victoriametrics.presto.model.VmSplit
import com.victoriametrics.presto.model.VmTableLayoutHandle
import javax.inject.Inject

class VmSplitManager
@Inject constructor(
        private val config: VmConfig
) : ConnectorSplitManager {
    override fun getSplits(
        transactionHandle: ConnectorTransactionHandle,
        session: ConnectorSession,
        layout: ConnectorTableLayoutHandle,
        splitSchedulingContext: ConnectorSplitManager.SplitSchedulingContext?
    ): FixedSplitSource {
        layout as VmTableLayoutHandle

        val constraint: TupleDomain<ColumnHandle> = layout.constraint

        val splits = listOf(VmSplit(config, constraint))

        return FixedSplitSource(splits)
    }
}

