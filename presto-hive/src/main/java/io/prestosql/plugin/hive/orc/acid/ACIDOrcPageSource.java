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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.prestosql.orc.ACIDConstants.PRESTO_ACID_BUCKET_INDEX;
import static io.prestosql.orc.ACIDConstants.PRESTO_ACID_META_COLS_COUNT;
import static io.prestosql.orc.ACIDConstants.PRESTO_ACID_ORIGINAL_TXN_INDEX;
import static io.prestosql.orc.ACIDConstants.PRESTO_ACID_ROWID_INDEX;
import static io.prestosql.plugin.hive.DeleteDeltaLocations.hasDeletedRows;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;

public class ACIDOrcPageSource
        extends OrcPageSource
{
    private DeletedRowsRegistry deletedRowsRegistry;
    private boolean deletedRowsPresent;

    public ACIDOrcPageSource(
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
        super(recordReader, orcDataSource, columns, typeManager, systemMemoryContext, stats);
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
    public Page getNextPage()
    {
        Page dataPage = super.getNextPage();
        if (dataPage == null) {
            return null;
        }

        if (deletedRowsPresent) {
            Block[] blocks = processAcidBlocks(dataPage);
            return new Page(dataPage.getPositionCount(), blocks);
        }
        else {
            Block[] blocks = new Block[dataPage.getChannelCount() + 1];
            for (int i = 0; i < dataPage.getChannelCount(); i++) {
                blocks[i] = dataPage.getBlock(i);
            }
            blocks[dataPage.getChannelCount()] = deletedRowsRegistry.createIsValidBlockForAllValid(dataPage.getPositionCount());
            return new Page(dataPage.getPositionCount(), blocks);
        }
    }

    private Block[] processAcidBlocks(Page page)
    {
        if (page.getChannelCount() < PRESTO_ACID_META_COLS_COUNT) {
            // There should atlest be 3 columns i.e ACID meta columns should be there at the least
            throw new PrestoException(HIVE_BAD_DATA, "Did not read enough blocks for ACID file: " + page.getChannelCount());
        }

        Block[] dataBlocks = new Block[page.getChannelCount() - PRESTO_ACID_META_COLS_COUNT + 1]; // We will return only data blocks and one isValid block
        // Copy data block references
        int colIdx = 0;
        for (int i = PRESTO_ACID_META_COLS_COUNT; i < page.getChannelCount(); i++) {
            dataBlocks[colIdx++] = page.getBlock(i);
        }

        dataBlocks[colIdx] = deletedRowsRegistry.createIsValidBlock(
                page.getBlock(PRESTO_ACID_ORIGINAL_TXN_INDEX),
                page.getBlock(PRESTO_ACID_BUCKET_INDEX),
                page.getBlock(PRESTO_ACID_ROWID_INDEX));

        return dataBlocks;
    }

    @VisibleForTesting
    public OrcRecordReader getRecordReader()
    {
        return recordReader;
    }
}
