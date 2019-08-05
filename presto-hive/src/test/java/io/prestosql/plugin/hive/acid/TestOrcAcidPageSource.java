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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.Nation;
import io.airlift.tpch.NationColumn;
import io.airlift.tpch.NationGenerator;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.airlift.tpch.NationColumn.COMMENT;
import static io.airlift.tpch.NationColumn.NAME;
import static io.airlift.tpch.NationColumn.NATION_KEY;
import static io.airlift.tpch.NationColumn.REGION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestHdfsEnvironment;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;

public class TestOrcAcidPageSource
{
    private static final HivePageSourceFactory PAGE_SOURCE_FACTORY = new OrcPageSourceFactory(
            new OrcReaderConfig(),
            createTestHdfsEnvironment(),
            new FileFormatDataSourceStats());

    @Test
    public void testFullFileRead()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.empty());
    }

    @Test
    public void testSingleColumnRead()
    {
        assertRead(ImmutableSet.of(REGION_KEY), OptionalLong.empty());
    }

    /**
     * tests file stats based pruning works fine
     */
    @Test
    public void testFullFileSkipped()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.of(100L));
    }

    /**
     * Tests stripe stats and row groups stats based pruning works fine
     */
    @Test
    public void testSomeStripesAndRowGroupRead()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.of(5L));
    }

    private static void assertRead(Set<NationColumn> columns, OptionalLong nationKeyPredicate)
    {
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.all();
        if (nationKeyPredicate.isPresent()) {
            tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(toHiveColumnHandle(NATION_KEY), Domain.singleValue(BIGINT, nationKeyPredicate.getAsLong())));
        }

        List<Nation> actual = readFile(columns, tupleDomain);

        List<Nation> expected = new ArrayList<>();
        for (Nation nation : ImmutableList.copyOf(new NationGenerator().iterator())) {
            if (!nationKeyPredicate.isPresent() || nationKeyPredicate.getAsLong() == nation.getNationKey()) {
                expected.addAll(nCopies(1000, nation));
            }
        }

        assertEqualsByColumns(columns, actual, expected);
    }

    private static List<Nation> readFile(Set<NationColumn> columns, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        List<HiveColumnHandle> columnHandles = columns.stream()
                .map(TestOrcAcidPageSource::toHiveColumnHandle)
                .collect(toImmutableList());

        List<String> columnNames = columnHandles.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());

        // This file has the contains the TPC-H nation table which each row repeated 1000 times
        final File nationFileWithReplicatedRows = new File(getResource("nationFile25kRowsSortedOnNationKey/bucket_00000").getPath());

        ConnectorPageSource pageSource = PAGE_SOURCE_FACTORY.createPageSource(
                new JobConf(new Configuration(false)),
                SESSION,
                new Path(nationFileWithReplicatedRows.getAbsoluteFile().toURI()),
                0,
                nationFileWithReplicatedRows.length(),
                nationFileWithReplicatedRows.length(),
                createSchema(),
                columnHandles,
                tupleDomain,
                DateTimeZone.UTC,
                Optional.empty()).get();

        int nationKeyColumn = columnNames.indexOf("n_nationkey");
        int nameColumn = columnNames.indexOf("n_name");
        int regionKeyColumn = columnNames.indexOf("n_regionkey");
        int commentColumn = columnNames.indexOf("n_comment");

        ImmutableList.Builder<Nation> rows = ImmutableList.builder();
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            page = page.getLoadedPage();
            for (int position = 0; position < page.getPositionCount(); position++) {
                long nationKey = -42;
                if (nationKeyColumn >= 0) {
                    nationKey = BIGINT.getLong(page.getBlock(nationKeyColumn), position);
                }

                String name = "<not read>";
                if (nameColumn >= 0) {
                    name = VARCHAR.getSlice(page.getBlock(nameColumn), position).toStringUtf8();
                }

                long regionKey = -42;
                if (regionKeyColumn >= 0) {
                    regionKey = BIGINT.getLong(page.getBlock(regionKeyColumn), position);
                }

                String comment = "<not read>";
                if (commentColumn >= 0) {
                    comment = VARCHAR.getSlice(page.getBlock(commentColumn), position).toStringUtf8();
                }

                rows.add(new Nation(position, nationKey, name, regionKey, comment));
            }
        }
        return rows.build();
    }

    private static HiveColumnHandle toHiveColumnHandle(NationColumn nationColumn)
    {
        Type prestoType;
        switch (nationColumn.getType().getBase()) {
            case IDENTIFIER:
            case INTEGER:
                prestoType = BIGINT;
                break;
            case VARCHAR:
                prestoType = VARCHAR;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + nationColumn.getType().getBase());
        }

        return new HiveColumnHandle(
                nationColumn.getColumnName(),
                toHiveType(new HiveTypeTranslator(), prestoType),
                prestoType,
                0,
                REGULAR,
                Optional.empty());
    }

    private static Properties createSchema()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ORC.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, ORC.getInputFormat());
        schema.setProperty(TABLE_IS_TRANSACTIONAL, "true");
        return schema;
    }

    private static void assertEqualsByColumns(Set<NationColumn> columns, List<Nation> actualRows, List<Nation> expectedRows)
    {
        assertEquals(actualRows.size(), expectedRows.size(), "row count");
        for (int i = 0; i < actualRows.size(); i++) {
            Nation actual = actualRows.get(i);
            Nation expected = expectedRows.get(i);
            assertEquals(actual.getNationKey(), columns.contains(NATION_KEY) ? expected.getNationKey() : -1);
            assertEquals(actual.getName(), columns.contains(NAME) ? expected.getName() : "<not read>");
            assertEquals(actual.getRegionKey(), columns.contains(REGION_KEY) ? expected.getRegionKey() : -1);
            assertEquals(actual.getComment(), columns.contains(COMMENT) ? expected.getComment() : "<not read>");
        }
    }
}
