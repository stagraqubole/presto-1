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
package io.prestosql.plugin.hive.metastore;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class HiveAcidState
{
    private Optional<Long> transactionId = Optional.empty();
    private AtomicBoolean transactionValidAtMetastore = new AtomicBoolean(true);
    private ScheduledFuture heartbeatTask;
    private String validWriteIds;
    private Duration heartbeatInterval;

    private static final Logger log = Logger.get(HiveAcidState.class);

    public HiveAcidState(Duration heartbeatInterval)
    {
        this.heartbeatInterval = heartbeatInterval;
    }

    public HiveAcidState setTransactionId(long transactionId, ConnectorSession session)
    {
        if (isTransactionOpen()) {
            throw new IllegalArgumentException("Setting another hive transaction while one is already open: " + this.transactionId.get());
        }
        this.transactionId = Optional.of(transactionId);

        return this;
    }

    public HiveAcidState setHeartbeatTask(ScheduledExecutorService heartbeater, HiveMetastore metastore)
    {
        checkArgument(isTransactionOpen(), "Hive transaction should be open before starting the Heartbeat");

        heartbeatTask = heartbeater.scheduleWithFixedDelay(
                new HiveAcidState.HeartbeatRunnable(transactionId.get(), metastore, transactionValidAtMetastore),
                (long) (heartbeatInterval.toMillis() * 0.75 * Math.random()),
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        return this;
    }

    public HiveAcidState setValidWriteIds(String validWriteIds)
    {
        this.validWriteIds = validWriteIds;
        return this;
    }

    public String getValidWriteIds()
    {
        return validWriteIds;
    }

    public boolean isTransactionOpen()
    {
        return transactionId.isPresent();
    }

    public long getTransactionId()
    {
        checkArgument(isTransactionOpen(), "Hive transaction not open");
        return transactionId.get();
    }

    public void endHeartbeat(ConnectorSession session)
    {
        log.info("Terminated heartbeat thread for query: " + session.getQueryId());
        heartbeatTask.cancel(true);
    }

    public boolean isTrasactionValidAtMetastore()
    {
        return transactionValidAtMetastore.get();
    }

    private static class HeartbeatRunnable
            implements Runnable
    {
        private final long transactionId;
        private final HiveMetastore metastore;
        private final AtomicBoolean valid;

        public HeartbeatRunnable(long transactionId, HiveMetastore metastore, AtomicBoolean valid)
        {
            this.transactionId = transactionId;
            this.metastore = metastore;
            this.valid = valid;
        }

        @Override
        public void run()
        {
            if (valid.get()) {
                valid.set(metastore.sendTransactionHeartbeatAndFindIfValid(transactionId));

                if (!valid.get()) {
                    log.error("Transaction Heartbeat: Transaction aborted by Metastore: " + transactionId);
                }
            }
        }
    }
}
