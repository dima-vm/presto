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

import com.facebook.presto.expressions.translator.RowExpressionTranslator
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator
import com.facebook.presto.expressions.translator.TranslatedExpression
import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.function.FunctionMetadataManager
import com.facebook.presto.spi.relation.CallExpression
import com.facebook.presto.spi.relation.ConstantExpression
import com.facebook.presto.spi.relation.LambdaDefinitionExpression
import com.facebook.presto.spi.relation.SpecialFormExpression
import com.facebook.presto.spi.relation.VariableReferenceExpression
import com.victoriametrics.presto.VmConnectorPlanOptimizer.VmExpression
import java.util.*
import javax.inject.Inject

class VmFilterToSqlTranslator @Inject constructor(val functionManager: FunctionMetadataManager) :
        RowExpressionTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>()
{
    override fun translateCall(
            call: CallExpression,
            context: Map<VariableReferenceExpression, ColumnHandle>,
            rowExpressionTreeTranslator: RowExpressionTreeTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>
    ): TranslatedExpression<VmExpression> {
        val translated = call.arguments.map {
            rowExpressionTreeTranslator.rewrite(it, context)
        }
        val functionMetadata = functionManager.getFunctionMetadata(call.functionHandle)
        // functionMapping.get(functionMetadata).invokeWithArguments(translatedArguments)
        VmExpression()
        return TranslatedExpression<VmExpression>(Optional.of(call.functionHandle), call, translated);
        // return super.translateCall(call, context, rowExpressionTreeTranslator)
    }

    override fun translateConstant(
            literal: ConstantExpression?,
            context: Map<VariableReferenceExpression, ColumnHandle>?,
            rowExpressionTreeTranslator: RowExpressionTreeTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>?
    ): TranslatedExpression<VmExpression> {
        return super.translateConstant(literal, context, rowExpressionTreeTranslator)
    }

    override fun translateVariable(
            variable: VariableReferenceExpression?,
            context: Map<VariableReferenceExpression, ColumnHandle>?,
            rowExpressionTreeTranslator: RowExpressionTreeTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>?
    ): TranslatedExpression<VmExpression> {
        return super.translateVariable(variable, context, rowExpressionTreeTranslator)
    }

    override fun translateLambda(
            lambda: LambdaDefinitionExpression?,
            context: Map<VariableReferenceExpression, ColumnHandle>?,
            rowExpressionTreeTranslator: RowExpressionTreeTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>?
    ): TranslatedExpression<VmExpression> {
        return super.translateLambda(lambda, context, rowExpressionTreeTranslator)
    }

    override fun translateSpecialForm(
            specialForm: SpecialFormExpression?,
            context: Map<VariableReferenceExpression, ColumnHandle>?,
            rowExpressionTreeTranslator: RowExpressionTreeTranslator<VmExpression, Map<VariableReferenceExpression, ColumnHandle>>?
    ): TranslatedExpression<VmExpression> {
        return super.translateSpecialForm(specialForm, context, rowExpressionTreeTranslator)
    }
}
