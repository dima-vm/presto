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

import com.facebook.presto.expressions.LogicalRowExpressions
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator
import com.facebook.presto.expressions.translator.TranslatedExpression
import com.facebook.presto.spi.ConnectorPlanOptimizer
import com.facebook.presto.spi.ConnectorSession
import com.facebook.presto.spi.VariableAllocator
import com.facebook.presto.spi.connector.ConnectorContext
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider
import com.facebook.presto.spi.function.FunctionHandle
import com.facebook.presto.spi.plan.FilterNode
import com.facebook.presto.spi.plan.PlanNode
import com.facebook.presto.spi.plan.PlanNodeIdAllocator
import com.facebook.presto.spi.plan.PlanVisitor
import com.facebook.presto.spi.plan.TableScanNode
import com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED
import com.facebook.presto.spi.relation.RowExpression
import com.victoriametrics.presto.model.VmTableHandle
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class VmConnectorPlanOptimizer @Inject constructor(
        val context: ConnectorContext
) : ConnectorPlanOptimizer {
    data class VmExpression(val a: FunctionHandle)

    override fun optimize(
            maxSubplan: PlanNode,
            session: ConnectorSession,
            variableAllocator: VariableAllocator,
            idAllocator: PlanNodeIdAllocator
    ): PlanNode {
        return maxSubplan.accept(Visitor(session, idAllocator), context)
    }

    inner class Visitor(
            val session: ConnectorSession,
            val idAllocator: PlanNodeIdAllocator
    ) : PlanVisitor<PlanNode, ConnectorContext>() {

        override fun visitPlan(node: PlanNode, context: ConnectorContext): PlanNode {
            val newChildren: List<PlanNode> = node.sources.map {
                it.accept(this, context)
            }
            return when (newChildren) {
                node.sources -> node
                else -> node.replaceChildren(newChildren)
            }
        }


        override fun visitFilter(node: FilterNode, context: ConnectorContext): PlanNode {
            val source = node.source as? TableScanNode ?: return node

            val connectorTableHandle: VmTableHandle = source.table.connectorHandle as VmTableHandle

            val assignments = source.assignments
            val c = source.currentConstraint // TupleDomain{All}
            val e = source.enforcedConstraint // TupleDomain{All}

            val vmFilterToSqlTranslator = SqlToVmFilterTranslator(
                    context.functionMetadataManager
                    // FunctionTranslator.buildFunctionTranslator(functionTranslators)
            )
            val determinismEvaluator = context.rowExpressionService.determinismEvaluator
            val logicalRowExpressions = LogicalRowExpressions(
                    determinismEvaluator,
                    context.standardFunctionResolution,
                    context.functionMetadataManager)

            val expressionOptimizer = context.rowExpressionService.expressionOptimizer
            var predicate: RowExpression = expressionOptimizer.optimize(node.predicate, OPTIMIZED, session)
            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate)

            // TODO if jdbcExpression is not present, walk through translated subtree to find out which parts can be pushed down
            // TODO if jdbcExpression is not present, walk through translated subtree to find out which parts can be pushed down
            // if (!tableHandle.getLayout().isPresent() || !jdbcExpression.getTranslated().isPresent()) {
            //     return node
            // }


            val translatedExpression: TranslatedExpression<VmExpression> = RowExpressionTreeTranslator.translateWith(
                    predicate,
                    vmFilterToSqlTranslator,
                    assignments)

            //
            // val tableHandle = TableHandle(
            //         source.table.connectorId,
            //         source.table.connectorHandle,
            //         source.table.transaction,
            //         source.table.layout)

            val newTableScanNode = TableScanNode(
                    idAllocator.nextId,
                    source.table,
                    source.outputVariables,
                    source.assignments,
                    source.currentConstraint,
                    source.enforcedConstraint)

            return FilterNode(idAllocator.nextId, newTableScanNode, node.predicate)


            // var predicate: RowExpression = expressionOptimizer.optimize(node.predicate, ExpressionOptimizer.Level.OPTIMIZED, session)
            // predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate)
            // val jdbcExpression: TranslatedExpression<JdbcExpression> = RowExpressionTreeTranslator.translateWith(
            //         predicate,
            //         jdbcFilterToSqlTranslator,
            //         oldTableScanNode.assignments)
            //
            // // TODO if jdbcExpression is not present, walk through translated subtree to find out which parts can be pushed down
            // // TODO if jdbcExpression is not present, walk through translated subtree to find out which parts can be pushed down
            // if (!oldTableHandle.layout.isPresent || !jdbcExpression.getTranslated().isPresent()) {
            //     return node
            // }
            //
            // val oldTableLayoutHandle: JdbcTableLayoutHandle = oldTableHandle.layout.get() as JdbcTableLayoutHandle
            // val newTableLayoutHandle = JdbcTableLayoutHandle(
            //         oldConnectorTable,
            //         oldTableLayoutHandle.getTupleDomain(),
            //         jdbcExpression.getTranslated())
            //
            // val tableHandle = TableHandle(
            //         oldTableHandle.connectorId,
            //         oldTableHandle.connectorHandle,
            //         oldTableHandle.transaction,
            //         Optional.of(newTableLayoutHandle))
            //
            // val newTableScanNode = TableScanNode(
            //         idAllocator.getNextId(),
            //         tableHandle,
            //         oldTableScanNode.outputVariables,
            //         oldTableScanNode.assignments,
            //         oldTableScanNode.currentConstraint,
            //         oldTableScanNode.enforcedConstraint)
            //
            //
            //
            // node.predicate
        }

    }

    @Singleton
    class Provider @Inject constructor(
            vmConnectorPlanOptimizer: VmConnectorPlanOptimizer
    ) : ConnectorPlanOptimizerProvider {
        val optimizersSet = setOf(vmConnectorPlanOptimizer)
        override fun getLogicalPlanOptimizers() = emptySet<ConnectorPlanOptimizer>()
        override fun getPhysicalPlanOptimizers() = optimizersSet
    }
}
