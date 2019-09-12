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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.AcidConstants.ACID_BUCKET_INDEX;
import static io.prestosql.orc.AcidConstants.ACID_ORIGINAL_TRANSACTION_INDEX;
import static io.prestosql.orc.AcidConstants.ACID_ROWID_INDEX;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_META_COLS_COUNT;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.plugin.hive.DeleteDeltaLocations.hasDeletedRows;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AcidOrcPageSource
        implements ConnectorPageSource
{
    private static final int NULL_ENTRY_SIZE = 0;
    protected final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;
    private final boolean deletedRowsPresent;

    private int batchId;
    private boolean closed;

    private final AggregatedMemoryContext systemMemoryContext;

    private final FileFormatDataSourceStats stats;

    private DeletedRowsRegistry deletedRowsRegistry;

    public AcidOrcPageSource(
            Path splitPath,
            OrcPageSourceFactory pageSourceFactory,
            ConnectorSession session,
            Configuration configuration,
            DateTimeZone hiveStorageTimeZone,
            HdfsEnvironment hdfsEnvironment,
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats,
            DataSize deletedRowsCacheSize,
            Duration deletedRowsCacheTTL,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
            throws ExecutionException
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        int size = requireNonNull(columns, "columns is null").size();

        this.stats = requireNonNull(stats, "stats is null");

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.getColumnType() == REGULAR, "column type must be regular");

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                constantBlocks[columnIndex] = RunLengthEncodedBlock.create(type, null, MAX_BATCH_SIZE);
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        deletedRowsRegistry = new DeletedRowsRegistry(
                splitPath,
                pageSourceFactory,
                session,
                configuration,
                hiveStorageTimeZone,
                hdfsEnvironment,
                deletedRowsCacheSize,
                deletedRowsCacheTTL,
                deleteDeltaLocations);
        this.deletedRowsPresent = hasDeletedRows(deleteDeltaLocations);
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        if (!deletedRowsPresent) {
            return getNextPageNormal();
        }

        try {
            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            ValidPositions validPositions = deletedRowsRegistry.getValidPositions(
                    batchSize,
                    recordReader.readBlock(ACID_ORIGINAL_TRANSACTION_INDEX),
                    recordReader.readBlock(ACID_BUCKET_INDEX),
                    recordReader.readBlock(ACID_ROWID_INDEX));

            Block[] blocks = new Block[hiveColumnIndexes.length - PRESTO_ACID_META_COLS_COUNT];
            for (int fieldId = PRESTO_ACID_META_COLS_COUNT; fieldId < hiveColumnIndexes.length; fieldId++) {
                int blockId = fieldId - PRESTO_ACID_META_COLS_COUNT;
                if (constantBlocks[fieldId] != null) {
                    blocks[blockId] = new AcidBlock(batchSize, constantBlocks[fieldId].getRegion(0, batchSize), validPositions);
                }
                else {
                    blocks[blockId] = new LazyBlock(batchSize, new AcidOrcBlockLoader(hiveColumnIndexes[fieldId], batchSize, validPositions));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
        }
    }

    public Page getNextPageNormal()
    {
        try {
            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    blocks[fieldId] = new LazyBlock(batchSize, new OrcBlockLoader(hiveColumnIndexes[fieldId]));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    @VisibleForTesting
    public OrcRecordReader getRecordReader()
    {
        return recordReader;
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    private final class AcidOrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private final ValidPositions validPositions;
        private final int totalPositions;
        private boolean loaded;

        public AcidOrcBlockLoader(int columnIndex, int totalPositions, ValidPositions validPositions)
        {
            this.columnIndex = columnIndex;
            this.validPositions = validPositions;
            this.totalPositions = totalPositions;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = recordReader.readBlock(columnIndex);
                lazyBlock.setBlock(new AcidBlock(totalPositions, block, validPositions));
            }
            catch (OrcCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (IOException | RuntimeException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
            }

            loaded = true;
        }
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = recordReader.readBlock(columnIndex);
                lazyBlock.setBlock(block);
            }
            catch (OrcCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (IOException | RuntimeException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
            }

            loaded = true;
        }
    }
}
