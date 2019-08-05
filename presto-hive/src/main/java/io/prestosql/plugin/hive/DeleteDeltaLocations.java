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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/*
 * Stores information about DELETE_DELTAs for a Partition
 */
public class DeleteDeltaLocations
{
    private String partitionLocation;
    private List<DeleteDeltaInfo> deleteDeltas;

    public DeleteDeltaLocations()
    {
        this.deleteDeltas = new ArrayList();
    }

    @JsonCreator
    public DeleteDeltaLocations(
            @JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<DeleteDeltaInfo> deleteDeltas)
    {
        this.partitionLocation = partitionLocation;
        this.deleteDeltas = deleteDeltas;
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<DeleteDeltaInfo> getDeleteDeltas()
    {
        return deleteDeltas;
    }

    public void addDeleteDelta(Path deleteDeltaPath, long minWriteId, long maxWriteId, int statementId)
    {
        String partitionPathFromDeleteDelta = deleteDeltaPath.getParent().toString();
        checkState(partitionLocation == null || partitionLocation.equals(partitionPathFromDeleteDelta),
                format("Partition location in DeleteDelta, %s, does not match stored location %s", deleteDeltaPath.getParent().toString(), partitionLocation));
        this.partitionLocation = partitionPathFromDeleteDelta;

        this.deleteDeltas.add(new DeleteDeltaInfo(minWriteId, maxWriteId, statementId));
    }

    public static boolean hasDeletedRows(Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        return deleteDeltaLocations.isPresent() && deleteDeltaLocations.get().getDeleteDeltas().size() > 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deleteDeltas);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DeleteDeltaLocations)) {
            return false;
        }

        DeleteDeltaLocations other = (DeleteDeltaLocations) o;

        if ((partitionLocation == null || other.partitionLocation == null) && partitionLocation != other.partitionLocation) {
            return false;
        }

        if (partitionLocation != null && other.partitionLocation != null && !partitionLocation.equals(other.partitionLocation)) {
            return false;
        }

        if (!deleteDeltas.equals(other.deleteDeltas)) {
            return false;
        }

        return true;
    }

    public static class DeleteDeltaInfo
    {
        private final long minWriteId;
        private final long maxWriteId;
        private final int statementId;

        @JsonCreator
        public DeleteDeltaInfo(
                @JsonProperty("minWriteId") long minWriteId,
                @JsonProperty("maxWriteId") long maxWriteId,
                @JsonProperty("statementId") int statementId)
        {
            this.minWriteId = minWriteId;
            this.maxWriteId = maxWriteId;
            this.statementId = statementId;
        }

        @JsonProperty
        public long getMinWriteId()
        {
            return minWriteId;
        }

        @JsonProperty
        public long getMaxWriteId()
        {
            return maxWriteId;
        }

        @JsonProperty
        public int getStatementId()
        {
            return statementId;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .addValue(minWriteId)
                    .addValue(maxWriteId)
                    .addValue(statementId)
                    .toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minWriteId, maxWriteId, statementId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof DeleteDeltaInfo)) {
                return false;
            }

            DeleteDeltaInfo other = (DeleteDeltaInfo) o;
            if (minWriteId == other.minWriteId && maxWriteId == other.maxWriteId && statementId == other.statementId) {
                return true;
            }

            return false;
        }
    }
}
