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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.AcidInfo;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_BUCKET_INDEX;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_META_COLS_COUNT;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_ORIGINAL_TRANSACTION_INDEX;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_ROWID_INDEX;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;

public class AcidOrcPageSource
        extends OrcPageSource
{
    private DeletedRowsRegistry deletedRowsRegistry;
    private boolean originalFilesPresent;
    private OriginalFilesRegistry originalFilesRegistry;
    // Row ID relative to all the original files of the same bucket ID before this file in lexicographic order
    private long originalFileRowId;

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
            Optional<AcidInfo> acidInfo)
            throws ExecutionException
    {
        super(recordReader, orcDataSource, columns, typeManager, systemMemoryContext, stats);

        checkState(acidInfo.isPresent() &&
                acidInfo.get().getDeleteDeltaLocations().map(DeleteDeltaLocations::hadDeletedRows).orElse(false),
                "OrcPageSource should be used when there are no deleted rows");

        originalFilesPresent = acidInfo.isPresent() && acidInfo.get().getOriginalFileLocations().isPresent();

        deletedRowsRegistry = new DeletedRowsRegistry(
                splitPath,
                pageSourceFactory,
                session,
                configuration,
                hiveStorageTimeZone,
                hdfsEnvironment,
                deletedRowsCacheSize,
                deletedRowsCacheTTL,
                acidInfo);

        if (originalFilesPresent) {
            originalFilesRegistry = new OriginalFilesRegistry(acidInfo.get().getOriginalFileLocations().get().getOriginalFiles(),
                    splitPath.getParent().toString(),
                    hdfsEnvironment, session, configuration, stats);
            originalFileRowId = originalFilesRegistry.getRowCount(splitPath);
        }
    }

    @Override
    public Page getNextPage()
    {
        Page page = super.getNextPage();

        if (page == null) {
            return page;
        }

        if (originalFilesPresent) {
            // To compare row ID of an original file with delete delta, we need to calculate the overall startRowID of
            // this page.
            // originalFileRowId -> starting row ID of the current original file.
            // recordReader.getFilePosition() -> returns the position in the current original file.
            // startRowID -> {Total number of rows before this original file} + {Row number in current file}
            long startRowID = originalFileRowId + recordReader.getFilePosition();

            ValidPositions validPositions = originalFilesRegistry.getValidPositions(deletedRowsRegistry.getDeletedRows(), page.getPositionCount(), startRowID);
            Block[] dataBlocks = new Block[page.getChannelCount()];
            int colIdx = 0;
            for (int i = 0; i < page.getChannelCount(); i++) {
                // LazyBlock is required to prevent DictionaryBlock from loading the dictionary block in constructor via getRetainedSize
                dataBlocks[colIdx++] = new LazyBlock(validPositions.getPositionCount(), new AcidOrcBlockLoader(validPositions, page.getBlock(i)));
            }
            return new Page(validPositions.getPositionCount(), dataBlocks);
        }

        ValidPositions validPositions = deletedRowsRegistry.getValidPositions(
                page.getPositionCount(),
                page.getBlock(PRESTO_ACID_ORIGINAL_TRANSACTION_INDEX),
                page.getBlock(PRESTO_ACID_BUCKET_INDEX),
                page.getBlock(PRESTO_ACID_ROWID_INDEX));
        Block[] dataBlocks = new Block[page.getChannelCount() - PRESTO_ACID_META_COLS_COUNT];
        int colIdx = 0;
        for (int i = PRESTO_ACID_META_COLS_COUNT; i < page.getChannelCount(); i++) {
            // LazyBlock is required to prevent DictionaryBlock from loading the dictionary block in constructor via getRetainedSize
            dataBlocks[colIdx++] = new LazyBlock(validPositions.getPositionCount(), new AcidOrcBlockLoader(validPositions, page.getBlock(i)));
        }

        return new Page(validPositions.getPositionCount(), dataBlocks);
    }

    private final class AcidOrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final ValidPositions validPositions;
        private final Block sourceBlock;
        private boolean loaded;

        public AcidOrcBlockLoader(ValidPositions validPositions, Block sourceBlock)
        {
            this.validPositions = validPositions;
            this.sourceBlock = sourceBlock;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            Block block = new DictionaryBlock(0, validPositions.getPositionCount(), sourceBlock, validPositions.getValidPositions(), false, randomDictionaryId());
            lazyBlock.setBlock(block);

            loaded = true;
        }
    }
}
