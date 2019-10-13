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
package io.prestosql.plugin.hive.acid;

import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.OriginalFileLocations;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.orc.acid.DeletedRowsRegistry;
import io.prestosql.plugin.hive.orc.acid.OriginalFilesRegistry;
import io.prestosql.plugin.hive.orc.acid.ValidPositions;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.plugin.hive.HiveTestUtils.createTestHdfsEnvironment;
import static org.testng.Assert.assertTrue;

public class TestOriginalFilesRegistry
{
    private static final HiveConfig CONFIG = new HiveConfig();
    private static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(CONFIG);
    private static final String tablePath = Thread.currentThread().getContextClassLoader().getResource("dummy_id_data_orc").getPath();

    private static final String partitionLocation = tablePath;
    private static final Configuration config = new JobConf(new Configuration(false));

    private static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

    private OriginalFilesRegistry originalFilesRegistry;

    @BeforeClass
    public void setup()
    {
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        List<OriginalFileLocations.OriginalFileInfo> originalFileInfos = new ArrayList<>();

        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo(tablePath + "/000000_0", 730));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo(tablePath + "/000001_0", 730));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo(tablePath + "/000002_0", 741));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo(tablePath + "/000002_0_copy_1", 768));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo(tablePath + "/000002_0_copy_2", 743));

        originalFilesRegistry = new OriginalFilesRegistry(originalFileInfos, partitionLocation, HDFS_ENVIRONMENT, SESSION, config, new FileFormatDataSourceStats());
    }

    @Test
    public void testGetRowCountSingleOriginalFileBucket()
    {
        assertTrue(originalFilesRegistry.getRowCount(new Path(tablePath + "/000001_0")) == 0, "Original file should have 0 as the starting row count");
    }

    @Test
    public void testGetRowCountMultipleOriginalFilesBucket()
    {
        // Bucket-2 has original files: 000002_0, 000002_0_copy_1. Each file original file has 4 rows.
        // So, starting row ID of 000002_0_copy_2 = row count of original files in Bucket-2 before it in lexicographic order.
        assertTrue(originalFilesRegistry.getRowCount(new Path(tablePath + "/000002_0_copy_2")) == 8, "Original file 000002_0_copy_2 should have 8 as the starting row count");
    }

    @Test
    public void testGetValidPositions()
    {
        AtomicReference<Set<DeletedRowsRegistry.RowId>> deletedRows = new AtomicReference<>();
        Set<DeletedRowsRegistry.RowId> rowIdSet = new HashSet<>();
        rowIdSet.add(new DeletedRowsRegistry.RowId(0, 53700194, 2));
        deletedRows.set(rowIdSet);
        int positions = 4;
        long startRowID = 0;

        ValidPositions validPositions = originalFilesRegistry.getValidPositions(deletedRows, positions, startRowID);
        assertTrue(validPositions.getPosition(0) == 0, "Error in fetching valid positions");
        assertTrue(validPositions.getPosition(1) == 1, "Error in fetching valid positions");
        assertTrue(validPositions.getPosition(2) == 3, "Row 2 is deleted so should not be present in valid positions");
    }
}
