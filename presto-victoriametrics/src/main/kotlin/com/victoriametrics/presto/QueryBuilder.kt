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
import com.facebook.presto.spi.predicate.Marker
import com.facebook.presto.spi.predicate.Marker.Bound.EXACTLY
import com.facebook.presto.spi.predicate.Range
import com.facebook.presto.spi.predicate.SortedRangeSet
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.relation.RowExpression
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmConfig
import io.airlift.slice.Slice
import okhttp3.HttpUrl
import javax.inject.Inject

class QueryBuilder
@Inject constructor(private val config: VmConfig) {

    private fun getBaseQuery(): HttpUrl {
        return config.httpUrls
                .random()
                .newBuilder()
                .addPathSegment("export")
                .build()
    }

    fun build(constraint: TupleDomain<ColumnHandle>): List<HttpUrl> {
        var queries = listOf(getBaseQuery())

        if (!constraint.columnDomains.isPresent) {
            return queries
        }

        queries = handleMetricName(constraint, queries)
        queries = handleLabels(constraint, queries)
        queries = handleTimestamps(constraint, queries)

        return queries
    }

    private fun handleMetricName(constraint: TupleDomain<ColumnHandle>, queries: List<HttpUrl>): List<HttpUrl> {
        val names = constraint.columnDomains.get()
                .filter { (it.column as VmColumnHandle).columnName == "name" }
                .map { it.domain.values }
                .filterIsInstance(SortedRangeSet::class.java)
                .flatMap { it.orderedRanges }
                .filter { it.isSingleValue }
                .map { it.singleValue as Slice }
                .map { it.toString(Charsets.UTF_8) }

        if (names.isEmpty()) {
            return queries
        }

        return names.flatMap { name ->
            queries.map { query ->
                query.newBuilder()
                        .addQueryParameter("match", "{__name__=\"$name\"}")
                        .build()
            }
        }
    }

    private fun handleLabels(constraint: TupleDomain<ColumnHandle>, queries: List<HttpUrl>): List<HttpUrl> {
        val names = constraint.columnDomains.get()
                .filter { (it.column as VmColumnHandle).columnName == "labels" }
                .map { it.domain.values }
                .filterIsInstance(SortedRangeSet::class.java)
                .flatMap { it.orderedRanges }
                .filter { it.isSingleValue }
                .map { it.singleValue as Slice }
                .map { it.toString(Charsets.UTF_8) }

        if (names.isEmpty()) {
            return queries
        }

        return names.flatMap { name ->
            queries.map { query ->
                query.newBuilder()
                        .addQueryParameter("match", "{__name__=\"$name\"}")
                        .build()
            }
        }
    }

    // TODO: check that presto engine still filters out values that are within the span (per unenforcedConstraints)
    private fun handleTimestamps(constraint: TupleDomain<ColumnHandle>, queries: List<HttpUrl>): List<HttpUrl> {
        val ranges = constraint.columnDomains.get()
                .filter { (it.column as VmColumnHandle).columnName == "timestamp" }
                .map { it.domain.values }
                .filterIsInstance(SortedRangeSet::class.java)
                .flatMap { it.orderedRanges }

        if (ranges.isEmpty()) {
            return queries
        }

        return ranges.flatMap { timestampRange ->
            val queryParams = toQueryParams(timestampRange)
            queries.map { query ->
                val builder = query.newBuilder()
                queryParams.forEach { (key, value) ->
                    builder.addQueryParameter(key, value)
                }
                builder.build()
            }
        }
    }

    private fun toQueryParams(range: Range): MutableMap<String, String> {
        val queryParams = mutableMapOf<String, String>()
        val low: Marker = range.low
        val high = range.high
        if (low.valueBlock.isPresent) {
            var value = (low.value as Long).toDouble()
            if (low.bound != EXACTLY)
                value++
            value /= 1000
            queryParams["start"] = "%.${3}f".format(value)
        }

        if (high.valueBlock.isPresent) {
            var value = (high.value as Long).toDouble()
            if (high.bound != EXACTLY)
                value--
            value /= 1000
            queryParams["end"] = "%.${3}f".format(value)
        }

        return queryParams
    }

    // /**
    //  * @return unenforced expression
    //  */
    // fun buildFromPushdown(filter: RowExpression): VmQuery {
    //     val baseQuery = getBaseQuery()
    //
    //     // if (filter !is CallExpression) {
    //     return VmQuery(listOf(baseQuery), unenforced = filter)
    //     // }
    //
    //
    //     // for (argument in filter.arguments) {
    //     //     if (argument is) {
    //     //
    //     //     }
    //     // }
    //     //
    //     //
    //     // filter.arguments
    //
    // }

    data class VmQuery(
            val urls: List<HttpUrl>,
            val unenforced: RowExpression,
            val constraint: TupleDomain<ColumnHandle> = TupleDomain.all()
    )
}
