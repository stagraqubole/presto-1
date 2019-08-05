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
package io.prestosql.plugin.hive.orc.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.TupleDomainOrcPredicate.ColumnReference;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.hive.HiveUtil.isDeserializerClass;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AcidOrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final OrcPageSourceFactory orcPageSourceFactory;
    private final DataSize deletedRowsCacheSize;
    private final Duration deletedRowsCacheTTL;

    @Inject
    public AcidOrcPageSourceFactory(TypeManager typeManager, HiveConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, OrcPageSourceFactory orcPageSourceFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.orcPageSourceFactory = orcPageSourceFactory;
        this.deletedRowsCacheSize = config.getDeleteDeltaCacheSize();
        this.deletedRowsCacheTTL = config.getDeleteDeltaCacheTTL();
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        boolean isFullAcid = AcidUtils.isFullAcidTable(((Map<String, String>) (((Map) schema))));
        if (!isFullAcid) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createAcidOrcPageSource(
                orcPageSourceFactory,
                hdfsEnvironment,
                session.getUser(),
                session,
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats,
                deletedRowsCacheSize,
                deletedRowsCacheTTL,
                deleteDeltaLocations));
    }

    public static ConnectorPageSource createAcidOrcPageSource(
            OrcPageSourceFactory pageSourceFactory,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            ConnectorSession session,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            DataSize deletedRowsCacheSize,
            Duration deletedRowsCacheTTL,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize);

            // We need meta columns to created rowIds if there are delete deltas present
            boolean deletedRowsPresent = deleteDeltaLocations.map(DeleteDeltaLocations::hadDeletedRows).orElse(false);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandlesAcid(columns, reader, path, deletedRowsPresent);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            // effective predicate should be updated to have new column index in the Domain because data columns are now shifted by 5 positions
            if (effectivePredicate.getDomains().isPresent()) {
                Map<HiveColumnHandle, Domain> predicateDomain = effectivePredicate.getDomains().get();
                ImmutableMap.Builder<HiveColumnHandle, Domain> newPredicateDomain = ImmutableMap.builder();
                for (Map.Entry<HiveColumnHandle, Domain> entry : predicateDomain.entrySet()) {
                    HiveColumnHandle columnHandle = entry.getKey();
                    Domain domain = entry.getValue();
                    for (HiveColumnHandle physicalColumn : physicalColumns) {
                        if (physicalColumn.getName().equals(columnHandle.getName())) {
                            newPredicateDomain.put(physicalColumn, domain);
                        }
                    }
                }
                effectivePredicate = TupleDomain.withColumnDomains(newPredicateDomain.build());
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build(), orcBloomFiltersEnabled);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    true);

            if (!deletedRowsPresent) {
                return new OrcPageSource(
                        recordReader,
                        orcDataSource,
                        physicalColumns,
                        typeManager,
                        systemMemoryUsage,
                        stats);
            }

            return new AcidOrcPageSource(
                    path,
                    pageSourceFactory,
                    session,
                    configuration,
                    hiveStorageTimeZone,
                    hdfsEnvironment,
                    recordReader,
                    orcDataSource,
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
                    stats,
                    deletedRowsCacheSize,
                    deletedRowsCacheTTL,
                    deleteDeltaLocations);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandlesAcid(List<HiveColumnHandle> columns, OrcReader reader, Path path, boolean metaColumnsNeeded)
    {
        // Always use column names from reader for Acid files

        verifyFileHasColumnNames(reader.getColumnNames(), path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMapAcid(reader);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        // Add all meta columns
        if (metaColumnsNeeded) {
            for (Map.Entry<String, Integer> entry : physicalNameOrdinalMap.entrySet()) {
                if (entry.getValue() > 4) {
                    // Data columns, skip in this step
                    continue;
                }

                HiveType hiveType = null;
                switch (entry.getKey()) {
                    case "operation":
                        // not needed right now, rowId is made of only originalTransaction, bucket and rowId
                        continue;
                    case "originalTransaction":
                        hiveType = HiveType.HIVE_LONG;
                        break;
                    case "bucket":
                        hiveType = HiveType.HIVE_INT;
                        break;
                    case "rowId":
                        hiveType = HiveType.HIVE_LONG;
                        break;
                    case "currentTransaction":
                        // not needed right now, rowId is made of only originalTransaction, bucket and rowId
                        continue;
                    default:
                        // do nothing for other columns
                        break;
                }
                physicalColumns.add(new HiveColumnHandle(
                        entry.getKey(),
                        hiveType,
                        hiveType.getTypeSignature(),
                        entry.getValue(),
                        REGULAR,
                        Optional.empty()));
            }
        }

        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger
                // than the number of columns in the file so the reader will fill it with nulls
                physicalOrdinal = nextMissingColumnIndex;
                nextMissingColumnIndex++;
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
        }
        return physicalColumns.build();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (!physicalColumnNames.isEmpty() && physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMapAcid(OrcReader reader)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        List<OrcType> types = reader.getFooter().getTypes();
        // This is the structure of Acid file
        // struct<operation:int, originalTransaction:bigint, bucket:int, rowId:bigint, currentTransaction:bigint, row:struct<TABLE COLUMNS>>
        // RootStruct is type[0], originalTransaction is type[1], ..., RowStruct is type[6], table column1 is type[7] and so on
        if (types.size() < 7) {
            throw new PrestoException(
                    HIVE_BAD_DATA,
                    "ORC file does not contain adequate column types for Acid file: " + types);
        }

        List<String> tableColumnNames = types.get(6).getFieldNames();
        int ordinal = 0;
        // Add Acid meta columns
        for (int i = 0; i < types.get(0).getFieldCount() - 1; i++) { // -1 to skip the row STRUCT of data columns
            // Keeping ordinals starting from 0 as it will match with OrcRecordReader.getStatisticsByColumnOrdinal data column ordinals
            physicalNameOrdinalMap.put(types.get(0).getFieldName(i), ordinal);
            ordinal++;
        }

        // Add Data columns
        for (String physicalColumnName : tableColumnNames) {
            // Keeping ordinals starting from 0 as it will match with OrcRecordReader.getStatisticsByColumnOrdinal data column ordinals
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }
}
