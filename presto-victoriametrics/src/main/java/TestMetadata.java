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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPushdownFilterResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.block.MethodHandleUtil;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import javax.inject.Inject;
import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class TestMetadata implements ConnectorMetadata {

	@Override
	public boolean isPushdownFilterSupported(ConnectorSession session, ConnectorTableHandle tableHandle) {
		return true;
	}

	@Override
	public ConnectorPushdownFilterResult pushdownFilter(
			ConnectorSession session,
			ConnectorTableHandle tableHandle,
			RowExpression filter,
			Optional<ConnectorTableLayoutHandle> currentLayoutHandle) {

		ConnectorTableLayoutHandle tableLayoutHandle = new ConnectorTableLayoutHandle() {};
		ConnectorTableLayout tableLayout = new ConnectorTableLayout(tableLayoutHandle);

		return new ConnectorPushdownFilterResult(tableLayout, filter);
	}


	private final SchemaTableName tableName = new SchemaTableName("testschema", "testtable");
	private final List<ColumnMetadata> columns;

	@Inject
	public TestMetadata(ConnectorContext context) {
		columns = Lists.newArrayList(new ColumnMetadata("testcolumn", getLabelsType(context)));
	}

	@Override
	public List<ConnectorTableLayoutResult> getTableLayouts(
			ConnectorSession session,
			ConnectorTableHandle table,
			Constraint<ColumnHandle> constraint,
			Optional<Set<ColumnHandle>> desiredColumns) {
		List<ColumnDomain<ColumnHandle>> columnDomains = constraint.getSummary().getColumnDomains().get();

		// This assert fails
		assert (!columnDomains.isEmpty());

		return Collections.emptyList();
	}

	private MapType getLabelsType(ConnectorContext context) {
		VarcharType keyType = VarcharType.VARCHAR;
		VarcharType valueType = VarcharType.VARCHAR;

		MethodHandle keyTypeNativeGetter = MethodHandleUtil.nativeValueGetter(keyType);
		MethodHandle keyNativeEquals = context.getTypeManager()
				.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
		MethodHandle keyNativeHashCode = context.getTypeManager()
				.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));

		MethodHandle keyBlockNativeEquals = MethodHandleUtil.compose(keyNativeEquals, keyTypeNativeGetter);
		MethodHandle keyBlockEquals = MethodHandleUtil.compose(
				keyNativeEquals,
				keyTypeNativeGetter,
				keyTypeNativeGetter
		);
		MethodHandle keyBlockHashCode = MethodHandleUtil.compose(keyNativeHashCode, keyTypeNativeGetter);

		return new MapType(
				keyType,
				valueType,
				keyBlockNativeEquals,
				keyBlockEquals,
				keyNativeHashCode,
				keyBlockHashCode
		);
	}

	@Override
	public List<String> listSchemaNames(ConnectorSession session) {
		return ImmutableList.of(tableName.getSchemaName());
	}

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
		return new ConnectorTableMetadata(tableName, columns);
	}

	@Override
	public ColumnMetadata getColumnMetadata(
			ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
		return columns.get(0);
	}

	@Override
	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
			ConnectorSession session, SchemaTablePrefix prefix) {
		return ImmutableMap.of(tableName, columns);
	}

	@Override
	public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
		return new ConnectorTableLayout(handle);
	}

	@Override
	public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
		return null;
	}

	@Override
	public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
		return null;
	}
}
