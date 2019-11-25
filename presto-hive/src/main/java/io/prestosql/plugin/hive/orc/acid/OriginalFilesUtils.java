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

import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.OriginalFileLocations.OriginalFileInfo;
import static java.util.Objects.requireNonNull;

public class OriginalFilesUtils
{
    private static final Map<String, Long> originalFileRows = new ConcurrentHashMap<>();
    private final List<OriginalFileInfo> originalFileNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorSession session;
    private final Configuration configuration;
    private final FileFormatDataSourceStats stats;
    private final String partitionLocation;

    public OriginalFilesUtils(List<OriginalFileInfo> originalFileNames,
                                 String partitionLocation, HdfsEnvironment hdfsEnvironment,
                                 ConnectorSession session,
                                 Configuration configuration,
                                 FileFormatDataSourceStats stats)
    {
        this.originalFileNames = requireNonNull(originalFileNames, "originalFileNames is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.session = requireNonNull(session, "session is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.partitionLocation = requireNonNull(partitionLocation, "partitionLocation is null");
    }

    /**
     * Utility method to read the ORC footer and return number of rows present in the file.
     */
    private Long getRowsInFile(Path path, long fileSize)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(session.getUser(), () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    getOrcMaxMergeDistance(session),
                    getOrcMaxBufferSize(session),
                    getOrcStreamBufferSize(session),
                    getOrcLazyReadSmallRanges(session),
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Could not create ORC data source for file: " + path.getName(), e);
        }

        OrcReader reader;
        try {
            reader = new OrcReader(orcDataSource, getOrcMaxMergeDistance(session), getOrcMaxBufferSize(session),
                    getOrcStreamBufferSize(session));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Could not create ORC reader for file: " + path.getName(), e);
        }
        return reader.getFooter().getNumberOfRows();
    }

    /**
     * Returns total number of rows present before the given original file in the same bucket.
     * example: if bucket-1 has original files
     *  000000_0 -> X0 rows
     *  000000_0_copy1 -> X1 rows
     *  000000_0_copy2 -> X2 rows
     *
     * for 000000_0_copy2, it returns (X0+X1)
     */
    public long getRowCount(Path reqPath)
    {
        long rowCount = 0;
        for (OriginalFileInfo originalFileInfo : originalFileNames) {
            Path path = new Path(this.partitionLocation + "/" + originalFileInfo.getName());
            try {
                // Check if the file belongs to the same bucket and comes before 'reqPath' in lexicographic order.
                if (isSameBucket(reqPath, path, configuration) && path.compareTo(reqPath) < 0) {
                    if (!originalFileRows.containsKey(originalFileInfo.getName())) {
                        originalFileRows.put(originalFileInfo.getName(), getRowsInFile(path, originalFileInfo.getFileSize()));
                    }
                    rowCount += originalFileRows.get(originalFileInfo.getName());
                }
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Could not get number of rows from file: " + path.getName(), e);
            }
        }
        return rowCount;
    }

    /**
     * Returns if both the file path belong to the same bucket.
     */
    public static boolean isSameBucket(Path path1, Path path2, Configuration configuration)
            throws IOException
    {
        return AcidUtils.parseBaseOrDeltaBucketFilename(path2, configuration).getBucketId() ==
                AcidUtils.parseBaseOrDeltaBucketFilename(path1, configuration).getBucketId();
    }

    /**
     * Returns all the valid row positions excluding deleted rows
     */
    // TODO: harmandeeps: for original files only read rowId from deleteDeltas and keep other Optional.empty()
    public ValidPositions getValidPositions(AtomicReference<Set<DeletedRowsRegistry.RowId>> deletedRows, int positions, long startRowID)
    {
        Iterator<DeletedRowsRegistry.RowId> iterator = deletedRows.get().iterator();
        List<Long> deletedRowIds = new ArrayList<>();
        while (iterator.hasNext()) {
            deletedRowIds.add(iterator.next().rowId);
        }
        int[] validPositions = new int[positions];
        if (validPositions == null || validPositions.length < positions) {
            validPositions = new int[positions];
        }

        int index = 0;
        long rowID = startRowID;
        for (int position = 0; position < positions; position++) {
            if (!deletedRowIds.contains(rowID)) {
                validPositions[index++] = position;
            }
            rowID++;
        }
        return new ValidPositions(validPositions, index);
    }
}
