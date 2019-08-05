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

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.prestosql.plugin.hive.authentication.NoHiveMetastoreAuthentication;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.Transport;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.context.ThreadLocalTestContextHolder;
import io.prestosql.tempto.fulfillment.table.MutableTablesState;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.tempto.Requirements.compose;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_Acid;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;

public class TestPartitionedInsertOnlyAcidTable
        extends ProductTest
        implements RequirementsProvider
{
    private static String tableName = "ORC_acid_single_int_column_partitioned";

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
        sb.append("   col INT");
        sb.append(") ");
        sb.append("PARTITIONED BY (part_col INT) ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get());
        }
        sb.append(" STORED AS ORC");
        sb.append(" TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true')");
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
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 1");

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));

        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 2");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).hasRowsCount(2);

        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 3");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(3, 2));
    }

    @Test(groups = {HIVE_Acid, PROFILE_SPECIFIC_TESTS})
    public void testFullAcidTableShouldFail()
    {
        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        String fullAcidTable = getNewTableName();
        hiveQueryExecutor.executeQuery(
                String.format("CREATE TABLE %s (col string) stored as ORC TBLPROPERTIES ('transactional'='true')",
                fullAcidTable));
        assertThat(() -> query("SELECT * FROM " + fullAcidTable))
                .failsWithMessage("Reading from Full Acid tables are not supported: default." + fullAcidTable);
    }

    @Test(groups = {HIVE_Acid, PROFILE_SPECIFIC_TESTS})
    public void testFilesForAbortedTransactionsIsIgnored()
            throws TException
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();
        String databaseName = "default";

        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 0");

        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 1");

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));

        // Simulate aborted transaction in Hive which has left behind a write directory and file
        simulateAbortedHiveTranscation(databaseName, tableNameInDatabase, "part_col=2");

        // Above simluation would have written to the part_col a new delta directory that corresponds to a aborted transaction but it should not be read
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));
    }

    /*
     * This simulates a aborted transaction which leaves behind a file in a partition of a table as follows:
     *  1. Open Transaction
     *  2. Rollback Transaction
     *  3. Create a new table with location as the given partition location + delta directory for the rolledback transaction
     *  4. Insert something to it which will create a file for this table
     *  5. This file is invalid for the original table because the transaction is aborted
     */
    private void simulateAbortedHiveTranscation(String database, String tableName, String partitionSpec)
            throws TException
    {
        ThriftHiveMetastoreClient client = createMetastoreClient();
        try {
            // 1.
            long transaction = client.openTransaction("test");

            AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(database, tableName);
            rqst.setTxnIds(Collections.singletonList(transaction));
            long writeId = client.allocateTableWriteIds(rqst).get(0).getWriteId();

            // 2.
            client.rollbackTransaction(transaction);

            // 3.
            Table t = client.getTable(database, tableName);
            String tableLocation = t.getSd().getLocation();
            String partitionLocation = tableLocation.endsWith("/") ? tableLocation + partitionSpec : tableLocation + "/" + partitionSpec;
            String deltaLocation = String.format("%s/delta_%s_%s_0000/",
                    partitionLocation,
                    String.format("%07d", writeId),
                    String.format("%07d", writeId));
            QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");

            String tmpTableName = getNewTableName();
            hiveQueryExecutor.executeQuery(String.format("CREATE EXTERNAL TABLE %s (col string) stored as ORC location '%s'",
                    tmpTableName,
                    deltaLocation));

            // 4.
            hiveQueryExecutor.executeQuery(String.format("INSERT INTO TABLE %s SELECT 'a'", tmpTableName));
            hiveQueryExecutor.executeQuery("DROP TABLE " + tmpTableName);
        }
        finally {
            client.close();
        }
    }

    private String getNewTableName()
    {
        return "table_" + UUID.randomUUID().toString().replace('-', '_');
    }

    private ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://hadoop-master:9083");
        return new ThriftHiveMetastoreClient(
                Transport.create(
                    HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                    Optional.empty(),
                    Optional.empty(),
                    10000,
                    new NoHiveMetastoreAuthentication()));
    }
}
