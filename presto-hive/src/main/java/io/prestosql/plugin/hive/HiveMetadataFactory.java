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
package io.prestosql.plugin.hive;

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.security.AccessControlMetadataFactory;
import io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.prestosql.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TXN_TIMEOUT;
import static org.apache.hadoop.hive.conf.HiveConf.getDefaultTimeUnit;
import static org.apache.hadoop.hive.conf.HiveConf.toTime;

public class HiveMetadataFactory
        implements Supplier<TransactionalMetadata>
{
    private static final Logger log = Logger.get(HiveMetadataFactory.class);

    private final boolean allowCorruptWritesForTesting;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> acidTransactionHeartbeatInterval;

    public static final String ACID_NOT_SUPPORTED_DURATION = "0s";

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
            HiveConfig hiveConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            @ForHive ExecutorService executorService,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            NodeVersion nodeVersion,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this(
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveConfig.getDateTimeZone(),
                hiveConfig.getMaxConcurrentFileRenames(),
                hiveConfig.getAllowCorruptWritesForTesting(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                typeManager,
                locationService,
                partitionUpdateCodec,
                executorService,
                typeTranslator,
                nodeVersion.toString(),
                accessControlMetadataFactory);
    }

    public HiveMetadataFactory(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            int maxConcurrentFileRenames,
            boolean allowCorruptWritesForTesting,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            long perTransactionCacheMaximumSize,
            Duration hiveTransactionHeartbeatInterval,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            TypeTranslator typeTranslator,
            String prestoVersion,
            AccessControlMetadataFactory accessControlMetadataFactory)
    {
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);

        String hiveServerTransactionTimeout = metastore.getConfigValue(HIVE_TXN_TIMEOUT.toString(), ACID_NOT_SUPPORTED_DURATION);
        // Metastore implementations not supporting getConfigValue would return back the default value
        if (hiveServerTransactionTimeout == ACID_NOT_SUPPORTED_DURATION) {
            acidTransactionHeartbeatInterval = Optional.empty();
            log.info("Hive Transactions are not supported by the Metastore used");
        }
        else {
            acidTransactionHeartbeatInterval = Optional.of(firstNonNull(
                    hiveTransactionHeartbeatInterval,
                    Duration.succinctNanos(toTime(hiveServerTransactionTimeout, getDefaultTimeUnit(HIVE_TXN_TIMEOUT), TimeUnit.NANOSECONDS) / 2)));
        }
    }

    @Override
    public HiveMetadata get()
    {
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                CachingHiveMetastore.memoizeMetastore(this.metastore, perTransactionCacheMaximumSize), // per-transaction cache
                renameExecution,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                acidTransactionHeartbeatInterval);

        return new HiveMetadata(
                metastore,
                hdfsEnvironment,
                partitionManager,
                timeZone,
                allowCorruptWritesForTesting,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                typeManager,
                locationService,
                partitionUpdateCodec,
                typeTranslator,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                accessControlMetadataFactory.create(metastore));
    }
}
