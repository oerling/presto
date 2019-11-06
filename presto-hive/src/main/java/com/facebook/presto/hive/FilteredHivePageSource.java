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
package com.facebook.presto.hive;

import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory.toFilterFunctions;
import static com.facebook.presto.orc.TupleDomainFilterUtils.toFilter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class FilteredHivePageSource
        extends HivePageSource
{
    public static final ConstantExpression TRUE_CONSTANT = new ConstantExpression(true, BOOLEAN);
    private List<HiveColumnHandle> columns;
    private TupleDomain<HiveColumnHandle> predicate;
    private RowExpression remainingPredicate;
    private TypeManager typeManager;
    private RowExpressionService rowExpressionService;
    private ConnectorSession session;
    private List<HivePageSourceProvider.ColumnMapping> columnMappings;

    public FilteredHivePageSource(
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> predicate,
            RowExpression remainingPredicate,
            Optional<HivePageSourceProvider.BucketAdaptation> bucketAdaptation,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            ConnectorSession session,
            ConnectorPageSource delegate)
    {
        super(columnMappings, bucketAdaptation, hiveStorageTimeZone, typeManager, delegate);
        this.columnMappings = columnMappings;
        this.columns = columns;
        this.predicate = predicate;
        this.remainingPredicate = remainingPredicate;
        this.typeManager = typeManager;
        this.rowExpressionService = rowExpressionService;
        this.session = session;
    }

    @Override
    public Page getNextPage()
    {
        Page page = super.getNextPage();
        if (page == null || page.getPositionCount() == 0) {
            return page;
        }

        int[] positions = new int[page.getPositionCount()];
        int positionCount = page.getPositionCount();
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }

        ImmutableMap<HiveColumnHandle, Domain> filters = ImmutableMap.of();
        Optional<List<ColumnDomain<HiveColumnHandle>>> columnDomains = predicate.getColumnDomains();
        if (columnDomains.isPresent()) {
            filters = columnDomains.get().stream().collect(toImmutableMap(ColumnDomain::getColumn, ColumnDomain::getDomain));
        }

        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            blocks[i] = page.getBlock(i);
            HiveColumnHandle columnHandle = columnMappings.get(i).getHiveColumnHandle();
            if (filters.containsKey(columnHandle)) {
                positionCount = filterBlock(blocks[i], columnHandle.getHiveType().getType(typeManager), toFilter(filters.get(columnHandle)), positions, positionCount);
            }
        }

        if (remainingPredicate != null && !(remainingPredicate.equals(TRUE_CONSTANT))) {
            Map<VariableReferenceExpression, InputReferenceExpression> variableToInput = columns.stream()
                    .collect(toImmutableMap(
                            hiveColumnIndex -> new VariableReferenceExpression(hiveColumnIndex.getName(), hiveColumnIndex.getHiveType().getType(typeManager)),
                            hiveColumnIndex -> new InputReferenceExpression(hiveColumnIndex.getHiveColumnIndex(), hiveColumnIndex.getHiveType().getType(typeManager))));

            List<FilterFunction> filterFunctions = toFilterFunctions(replaceExpression(remainingPredicate, variableToInput), session, rowExpressionService.getDeterminismEvaluator(), rowExpressionService.getPredicateCompiler());
            RuntimeException[] errors = new RuntimeException[positionCount];

            for (FilterFunction function : filterFunctions) {
                positionCount = function.filter(page, positions, positionCount, errors);
                if (positionCount == 0) {
                    break;
                }
            }
            for (int i = 0; i < positionCount; i++) {
                if (errors[i] != null) {
                    throw errors[i];
                }
            }
        }
        page = new Page(positionCount, blocks);
        return page.getPositions(positions, 0, positionCount);
    }

    public static int filterBlock(Block block, Type type, TupleDomainFilter filter, int[] positions, int positionCount)
    {
        int outputPositionsCount = 0;
        if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT || type == TIMESTAMP) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else if (filter.testLong(type.getLong(block, position))) {
                    positions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }
        }
        else if (type == DoubleType.DOUBLE) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else if (filter.testDouble(longBitsToDouble(block.getLong(position)))) {
                    positions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }
        }
        else if (type == REAL) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else if (filter.testFloat(intBitsToFloat(block.getInt(position)))) {
                    positions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }
        }
        else if (isDecimalType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else {
                    if (((DecimalType) type).isShort()) {
                        if (filter.testLong(block.getLong(position))) {
                            positions[outputPositionsCount] = position;
                            outputPositionsCount++;
                        }
                    }
                    else if (filter.testDecimal(block.getLong(position, 0), block.getLong(position, Long.BYTES))) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
            }
        }
        else if (isVarcharType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
            }
        }
        else if (isCharType(type)) {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        positions[outputPositionsCount] = position;
                        outputPositionsCount++;
                    }
                }
            }
        }
        else {
            throw new UnsupportedOperationException("BlockStreamReader of " + type.toString() + " not supported");
        }

        return outputPositionsCount;
    }

    public static boolean isVarcharType(Type type)
    {
        return type instanceof VarcharType;
    }

    public static boolean isCharType(Type type)
    {
        return type instanceof CharType;
    }

    public static boolean isDecimalType(Type type)
    {
        return type instanceof DecimalType;
    }
}
