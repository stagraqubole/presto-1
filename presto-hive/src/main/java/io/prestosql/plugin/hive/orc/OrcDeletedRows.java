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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableSet;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;

@NotThreadSafe
public class OrcDeletedRows
{
    private final String sourceFileName;
    private final DeleteDeltaLocations deleteDeltaLocations;
    private final OrcDeletedDeltaPageSourceFactory pageSourceFactory;
    private final String sessionUser;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;

    private Set<RowId> deletedRows;

    public OrcDeletedRows(
            String sourceFileName,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            OrcDeletedDeltaPageSourceFactory pageSourceFactory,
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment)
    {
        this.sourceFileName = sourceFileName;
        this.pageSourceFactory = pageSourceFactory;
        this.sessionUser = sessionUser;
        this.configuration = configuration;
        this.hdfsEnvironment = hdfsEnvironment;
        if (deleteDeltaLocations.isPresent()) {
            this.deleteDeltaLocations = deleteDeltaLocations.get();
        }
        else {
            this.deletedRows = ImmutableSet.of();
            this.deleteDeltaLocations = null;
        }
    }

    public MaskDeletedRowsFunction getMaskDeletedRowsFunction(Page pageRowIds)
    {
        return new MaskDeletedRowsFunction(pageRowIds);
    }

    @NotThreadSafe
    public class MaskDeletedRowsFunction
            implements Function<Block, Block>
    {
        private Page pageRowIds;
        private int positionCount;
        private int[] validPositions;

        public MaskDeletedRowsFunction(Page pageRowIds)
        {
            this.pageRowIds = pageRowIds;
        }

        public int getPositionCount()
        {
            if (pageRowIds != null) {
                loadValidPositions();
            }

            return positionCount;
        }

        @Override
        public Block apply(Block block)
        {
            if (pageRowIds != null) {
                loadValidPositions();
            }

            if (positionCount == block.getPositionCount()) {
                return block;
            }
            return new DictionaryBlock(positionCount, block, validPositions);
        }

        private void loadValidPositions()
        {
            Set<RowId> deletedRows = getDeletedRows();
            if (deletedRows.isEmpty()) {
                this.positionCount = pageRowIds.getPositionCount();
                this.pageRowIds = null;
                return;
            }

            int[] validPositions = new int[pageRowIds.getPositionCount()];
            int validPositionsIndex = 0;
            for (int position = 0; position < pageRowIds.getPositionCount(); position++) {
                if (!deletedRows.contains(new RowId(pageRowIds, position))) {
                    validPositions[validPositionsIndex++] = position;
                }
            }
            this.positionCount = validPositionsIndex;
            this.validPositions = validPositions;
            this.pageRowIds = null;
        }
    }

    private Set<RowId> getDeletedRows()
    {
        if (deletedRows != null) {
            return deletedRows;
        }

        ImmutableSet.Builder<RowId> deletedRowsBuilder = ImmutableSet.builder();
        for (DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo : deleteDeltaLocations.getDeleteDeltas()) {
            Path path = createPath(deleteDeltaLocations.getPartitionLocation(), deleteDeltaInfo, sourceFileName);
            try {
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
                FileStatus fileStatus = fileSystem.getFileStatus(path);

                ConnectorPageSource pageSource = pageSourceFactory.createPageSource(fileStatus.getPath(), fileStatus.getLen());
                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (page != null) {
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            deletedRowsBuilder.add(new RowId(page, i));
                        }
                    }
                }
            }
            catch (FileNotFoundException ignored) {
                // source file does not have a delta delete file in this location
                continue;
            }
            catch (PrestoException e) {
                throw e;
            }
            catch (OrcCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (RuntimeException | IOException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", path), e);
            }
        }
        deletedRows = deletedRowsBuilder.build();
        return deletedRows;
    }

    private static Path createPath(String partitionLocation, DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo, String fileName)
    {
        Path directory = new Path(partitionLocation, deleteDeltaSubdir(
                deleteDeltaInfo.getMinWriteId(),
                deleteDeltaInfo.getMaxWriteId(),
                deleteDeltaInfo.getStatementId()));
        return new Path(directory, fileName);
    }

    private static class RowId
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowId.class).instanceSize() +
                ClassLayout.parseClass(Byte.class).instanceSize() * 2 +
                ClassLayout.parseClass(Integer.class).instanceSize();

        private final long originalTransaction;
        private final int bucket;
        private final long rowId;

        private RowId(Page page, int position)
        {
            this(BIGINT.getLong(page.getBlock(0), position),
                    (int) INTEGER.getLong(page.getBlock(1), position),
                    BIGINT.getLong(page.getBlock(2), position));
        }

        public RowId(long originalTransaction, int bucket, long rowId)
        {
            this.originalTransaction = originalTransaction;
            this.bucket = bucket;
            this.rowId = rowId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RowId rowId1 = (RowId) o;
            return originalTransaction == rowId1.originalTransaction &&
                    bucket == rowId1.bucket &&
                    rowId == rowId1.rowId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalTransaction, bucket, rowId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("originalTransaction", originalTransaction)
                    .add("bucket", bucket)
                    .add("rowId", rowId)
                    .toString();
        }
    }
}
