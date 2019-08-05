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
package io.prestosql.tests.hive.acid;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.context.ThreadLocalTestContextHolder;
import io.prestosql.tempto.fulfillment.table.MutableTablesState;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.tempto.Requirements.compose;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_Acid;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.hive.acid.AcidTestHelper.simulateAbortedHiveTranscation;
import static java.lang.String.format;

public class TestPartitionedFullAcidTable
        extends ProductTest
        implements RequirementsProvider
{
    private static String tableName = "ORC_acid_single_int_column_partitioned_fullacid";

    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_ORC = singleIntColumnPartitionedTableDefinition(Optional.empty());

    private static HiveTableDefinition singleIntColumnPartitionedTableDefinition(Optional<String> serde)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(buildSingleIntColumnPartitionedTableDDL(serde))
                .setNoData()
                .build();
    }

    private static String buildSingleIntColumnPartitionedTableDDL(Optional<String> rowFormat)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE %EXTERNAL% TABLE IF NOT EXISTS %NAME%(");
        sb.append("   col INT,");
        sb.append("   fcol INT");
        sb.append(") ");
        sb.append("PARTITIONED BY (part_col INT) ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get());
        }
        sb.append(" STORED AS ORC");
        sb.append(" TBLPROPERTIES ('transactional'='true')");
        return sb.toString();
    }

    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(mutableTable(SINGLE_INT_COLUMN_PARTITIONED_ORC));
    }

    @Test(groups = {HIVE_Acid, PROFILE_SPECIFIC_TESTS})
    public void testReadingPartitionedInsertOnlyAcidTable()
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();

        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 21, 1");
        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=1) select 11, 1");

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(21, 1, 2));

        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 22, 2");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).hasRowsCount(2);

        // test filtering
        onePartitionQueryResult = query(selectFromOnePartitionsSql + " AND fcol = 1");
        assertThat(onePartitionQueryResult).containsOnly(row(21, 1, 2));

        // delete a row
        hiveQueryExecutor.executeQuery(
                "DELETE FROM " + tableNameInDatabase + " where part_col=2 and fcol=2");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(21, 1, 2));

        // update the existing row
        hiveQueryExecutor.executeQuery(
                "UPDATE " + tableNameInDatabase + " set col = 23 where part_col=2 and fcol=1");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(23, 1, 2));
    }

    @Test(groups = {HIVE_Acid, PROFILE_SPECIFIC_TESTS})
    public void testFilesForAbortedTransactionsIsIgnored()
            throws TException
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();
        String databaseName = "default";

        // Normal test have some data
        testReadingPartitionedInsertOnlyAcidTable();

        // Simulate aborted transaction in Hive which has left behind a write directory and file
        simulateAbortedHiveTranscation(databaseName, tableNameInDatabase, "part_col=2");

        // Above simluation would have written to the part_col a new delta directory that corresponds to a aborted transaction but it should not be read
        assertThat(query("SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2")).containsOnly(row(23, 1, 2));
    }
}
