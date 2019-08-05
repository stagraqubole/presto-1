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
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TypeManager;

import java.util.List;

import static io.prestosql.orc.AcidConstants.PRESTO_ACID_META_COLS_COUNT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

public class AcidOrcPageSource
        extends OrcPageSource
{
    private Block isValidBlock;

    public AcidOrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        super(recordReader, orcDataSource, columns, typeManager, systemMemoryContext, stats);
    }

    @Override
    public Page getNextPage()
    {
        Page dataPage = super.getNextPage();
        if (dataPage == null) {
            return null;
        }

        Block[] blocks = processAcidBlocks(dataPage);
        return new Page(dataPage.getPositionCount(), blocks);
    }

    private Block[] processAcidBlocks(Page page)
    {
        if (page.getChannelCount() < PRESTO_ACID_META_COLS_COUNT) {
            // There should atlest be 3 columns i.e Acid meta columns should be there at the least
            throw new PrestoException(HIVE_BAD_DATA, "Did not read enough blocks for Acid file: " + page.getChannelCount());
        }

        Block[] dataBlocks = new Block[page.getChannelCount() - PRESTO_ACID_META_COLS_COUNT + 1]; // We will return only data blocks and one isValid block
        // Copy data block references
        int colIdx = 0;
        for (int i = PRESTO_ACID_META_COLS_COUNT; i < page.getChannelCount(); i++) {
            dataBlocks[colIdx++] = page.getBlock(i);
        }

        int positions = page.getPositionCount();
        if (isValidBlock == null || isValidBlock.getPositionCount() < positions) {
            isValidBlock = BOOLEAN.createFixedSizeBlockBuilder(positions);
            BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(positions);
            for (int i = 0; i < positions; i++) {
                BOOLEAN.writeBoolean(builder, true);
            }
            isValidBlock = builder.build();
        }

        dataBlocks[colIdx] = isValidBlock.getRegion(0, positions);

        return dataBlocks;
    }

    @VisibleForTesting
    public OrcRecordReader getRecordReader()
    {
        return recordReader;
    }
}
