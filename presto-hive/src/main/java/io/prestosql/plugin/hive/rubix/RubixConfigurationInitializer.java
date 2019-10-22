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
package io.prestosql.plugin.hive.rubix;

import com.qubole.rubix.presto.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.presto.CachingPrestoNativeAzureFileSystem;
import com.qubole.rubix.presto.CachingPrestoS3FileSystem;
import com.qubole.rubix.presto.PrestoClusterManager;
import com.qubole.rubix.spi.CacheConfig;
import io.prestosql.plugin.hive.ConfigurationInitializer;
import io.prestosql.spi.HostAddress;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.ClusterType.PRESTO_CLUSTER_MANAGER;

public class RubixConfigurationInitializer
        implements ConfigurationInitializer
{
    private static final String RUBIX_S3_FS_CLASS_NAME = CachingPrestoS3FileSystem.class.getName();
    private static final String RUBIX_AZURE_FS_CLASS_NAME = CachingPrestoNativeAzureFileSystem.class.getName();
    private static final String RUBIX_GS_FS_CLASS_NAME = CachingPrestoGoogleHadoopFileSystem.class.getName();

    private final boolean parallelWarmupEnabled;
    private final String cacheLocation;

    // Configs below are dependent on node joining the cluster
    private boolean cacheNotReady = true;
    private boolean isMaster;
    private HostAddress masterAddress;
    private String nodeAddress;

    @Inject
    public RubixConfigurationInitializer(RubixConfig config)
    {
        this.parallelWarmupEnabled = config.isParallelWarmupEnabled();
        this.cacheLocation = config.getCacheLocation();
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        if (cacheNotReady) {
            CacheConfig.setCacheDataEnabled(config, false);
            return;
        }

        updateConfiguration(config);
    }

    public Configuration updateConfiguration(Configuration config)
    {
        checkState(masterAddress != null, "masterAddress is not set");
        CacheConfig.setCacheDataEnabled(config, true);
        CacheConfig.setOnMaster(config, isMaster);
        CacheConfig.setCoordinatorHostName(config, masterAddress.getHostText());
        PrestoClusterManager.setPrestoServerPort(config, masterAddress.getPort());
        CacheConfig.setCurrentNodeHostName(config, nodeAddress);

        CacheConfig.setIsParallelWarmupEnabled(config, parallelWarmupEnabled);
        CacheConfig.setCacheDataDirPrefix(config, cacheLocation);

        CacheConfig.setEmbeddedMode(config, true);
        CacheConfig.setRubixClusterType(config, PRESTO_CLUSTER_MANAGER.ordinal());
        CacheConfig.enableHeartbeat(config, false);
        CacheConfig.setClusterNodeRefreshTime(config, 10);
        CacheConfig.setClusterNodesFetchRetryCount(config, Integer.MAX_VALUE);
        CacheConfig.setWorkerNodeInfoExpiryPeriod(config, 1);

        config.set("fs.s3.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3a.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3n.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.wasb.impl", RUBIX_AZURE_FS_CLASS_NAME);
        config.set("fs.gs.impl", RUBIX_GS_FS_CLASS_NAME);
        return config;
    }

    public void setMaster(boolean master)
    {
        isMaster = master;
    }

    public void setMasterAddress(HostAddress masterAddress)
    {
        this.masterAddress = masterAddress;
    }

    public void setCurrentNodeAddress(String nodeAddress)
    {
        this.nodeAddress = nodeAddress;
    }

    public void initializationDone()
    {
        checkState(masterAddress != null, "masterAddress is not set");
        cacheNotReady = false;
    }
}
