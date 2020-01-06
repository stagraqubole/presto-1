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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
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
    private final OrcDeleteDeltaPageSourceFactory pageSourceFactory;
    private final String sessionUser;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;

    private Set<RowId> deletedRows;

    public OrcDeletedRows(
            String sourceFileName,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            OrcDeleteDeltaPageSourceFactory pageSourceFactory,
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

    public MaskDeletedRowsFunction getMaskDeletedRowsFunction(Page sourcePage)
    {
        return new MaskDeletedRowsFunction(sourcePage);
    }

    @NotThreadSafe
    public class MaskDeletedRowsFunction
    {
        @Nullable
        private Page sourcePage;
        private int positionCount;
        @Nullable
        private int[] validPositions;

        public MaskDeletedRowsFunction(Page sourcePage)
        {
            this.sourcePage = sourcePage;
        }

        public int getPositionCount()
        {
            if (sourcePage != null) {
                loadValidPositions();
                verify(sourcePage == null);
            }

            return positionCount;
        }

        public Block apply(Block block)
        {
            if (sourcePage != null) {
                loadValidPositions();
                verify(sourcePage == null);
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
                this.positionCount = sourcePage.getPositionCount();
                this.sourcePage = null;
                return;
            }

            int[] validPositions = new int[sourcePage.getPositionCount()];
            int validPositionsIndex = 0;
            for (int position = 0; position < sourcePage.getPositionCount(); position++) {
                if (!deletedRows.contains(new RowId(sourcePage, position))) {
                    validPositions[validPositionsIndex] = position;
                    validPositionsIndex++;
                }
            }
            this.positionCount = validPositionsIndex;
            this.validPositions = validPositions;
            this.sourcePage = null;
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
                FileStatus fileStatus = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.getFileStatus(path));

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
                throw new PrestoException(HIVE_BAD_DATA, format("Failed to read ORC file: %s", path), e);
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
        private static final int ORIGINAL_TRANSACTION_INDEX = 0;
        private static final int BUCKET_ID_INDEX = 1;
        private static final int ROW_ID_INDEX = 2;

        private final long originalTransaction;
        private final int bucket;
        private final long rowId;

        private RowId(Page page, int position)
        {
            this.originalTransaction = BIGINT.getLong(page.getBlock(ORIGINAL_TRANSACTION_INDEX), position);
            this.bucket = (int) INTEGER.getLong(page.getBlock(BUCKET_ID_INDEX), position);
            this.rowId = BIGINT.getLong(page.getBlock(ROW_ID_INDEX), position);
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

            RowId other = (RowId) o;
            return originalTransaction == other.originalTransaction &&
                    bucket == other.bucket &&
                    rowId == other.rowId;
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
