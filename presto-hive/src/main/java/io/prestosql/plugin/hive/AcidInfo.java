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
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;

/*
 * Stores ACID related information like DELETE_DELTAs and ORIGINAL_FILEs for a Partition
 */
public class AcidInfo
{
    private final Optional<DeleteDeltaLocations> deleteDeltaLocations;
    private final Optional<OriginalFileLocations> originalFileLocations;
    private final Optional<Long> bucketId;

    @JsonCreator
    public AcidInfo(@JsonProperty("deleteDeltas") Optional<DeleteDeltaLocations> deleteDeltaLocations,
                    @JsonProperty("originalFiles") Optional<OriginalFileLocations> originalFileLocations,
                    @JsonProperty("bucketId") Optional<Long> bucketId)
    {
        this.deleteDeltaLocations = deleteDeltaLocations;
        this.originalFileLocations = originalFileLocations;
        this.bucketId = bucketId;
    }

    @JsonProperty
    public Optional<DeleteDeltaLocations> getDeleteDeltaLocations()
    {
        return deleteDeltaLocations;
    }

    @JsonProperty
    public Optional<OriginalFileLocations> getOriginalFileLocations()
    {
        return originalFileLocations;
    }

    @JsonProperty
    public Optional<Long> getBucketId()
    {
        return bucketId;
    }

    public boolean isSameBucket(Path path1, Path path2, Configuration conf)
            throws IOException
    {
        if (bucketId.isPresent()) {
            return bucketId.get() == AcidUtils.parseBaseOrDeltaBucketFilename(path2, conf).getBucketId();
        }
        return AcidUtils.parseBaseOrDeltaBucketFilename(path2, conf).getBucketId() ==
                AcidUtils.parseBaseOrDeltaBucketFilename(path1, conf).getBucketId();
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

        AcidInfo acidInfo = (AcidInfo) o;

        if (!deleteDeltaLocations.equals(acidInfo.deleteDeltaLocations)) {
            return false;
        }
        if (!originalFileLocations.equals(acidInfo.originalFileLocations)) {
            return false;
        }
        return bucketId.equals(acidInfo.bucketId);
    }

    @Override
    public int hashCode()
    {
        int result = deleteDeltaLocations.hashCode();
        result = 31 * result + originalFileLocations.hashCode();
        result = 31 * result + bucketId.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("deleteDeltaLocations", deleteDeltaLocations)
                .add("originalFileLocations", originalFileLocations)
                .add("bucketId", bucketId)
                .toString();
    }

    public static class Builder
    {
        private Optional<DeleteDeltaLocations> deleteDeltaLocations;
        private Optional<OriginalFileLocations> originalFileLocations;
        private Optional<Long> bucketId;
        private final Map<Long, OriginalFileLocations.Builder> bucketIdToOriginalFilesMap = new HashMap<>();
        private final Map<Path, Long> pathToBucketIdMap = new HashMap<>();
        private final Path paritionLocation;

        public Builder(Path paritionLocation)
        {
            this.deleteDeltaLocations = Optional.empty();
            this.originalFileLocations = Optional.empty();
            this.bucketId = Optional.empty();
            this.paritionLocation = paritionLocation;
        }

        public Builder(Configuration configuration, Path paritionLocation,
                List<HadoopShims.HdfsFileStatusWithId> originalFileLocations)
        {
            this(paritionLocation);
            long index = 0;
            for (HadoopShims.HdfsFileStatusWithId hdfsFileStatusWithId : originalFileLocations) {
                Path originalFilePath = hdfsFileStatusWithId.getFileStatus().getPath();
                long originalFileLength = hdfsFileStatusWithId.getFileStatus().getLen();
                try {
                    long bucketId = AcidUtils.parseBaseOrDeltaBucketFilename(originalFilePath, configuration).getBucketId();
                    bucketId = (bucketId == -1) ? index : bucketId;
                    OriginalFileLocations.Builder builder = bucketIdToOriginalFilesMap.getOrDefault(bucketId, new OriginalFileLocations.Builder(this.paritionLocation));
                    builder.addOriginalFileInfo(originalFilePath.toString(), originalFileLength);
                    bucketIdToOriginalFilesMap.put(bucketId, builder);
                    pathToBucketIdMap.put(originalFilePath, bucketId);
                    index++;
                }
                catch (IOException e) {
                    throw new PrestoException(HIVE_UNKNOWN_ERROR, String.format("Error in fetching bucket ID of original file: %s", originalFilePath), e);
                }
            }
        }

        public Builder deleteDeltaLocations(Path path, List<AcidUtils.ParsedDelta> currentDirectories)
        {
            if (currentDirectories == null || currentDirectories.isEmpty()) {
                return this;
            }
            DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder = new DeleteDeltaLocations.Builder(path);
            for (AcidUtils.ParsedDelta delta : currentDirectories) {
                if (delta.isDeleteDelta()) {
                    deleteDeltaLocationsBuilder.addDeleteDelta(delta.getPath(), delta.getMinWriteId(),
                            delta.getMaxWriteId(), delta.getStatementId());
                }
            }
            this.deleteDeltaLocations = Optional.of(deleteDeltaLocationsBuilder.build());
            return this;
        }

        public Builder deleteDeltaLocations(Optional<DeleteDeltaLocations> deleteDeltaLocations)
        {
            this.deleteDeltaLocations = deleteDeltaLocations;
            return this;
        }

        public Optional<DeleteDeltaLocations> getDeleteDeltaLocations()
        {
            return deleteDeltaLocations;
        }

        public Builder originalFileInfo(Optional<OriginalFileLocations> originalFileLocations)
        {
            this.originalFileLocations = originalFileLocations;
            return this;
        }

        public Optional<OriginalFileLocations> getOriginalFileLocations()
        {
            return originalFileLocations;
        }

        public Optional<Long> getBucketId()
        {
            return bucketId;
        }

        public Builder bucketId(Optional<Long> bucketId)
        {
            this.bucketId = bucketId;
            return this;
        }

        public AcidInfo build()
        {
            return new AcidInfo(this.deleteDeltaLocations, this.originalFileLocations, this.bucketId);
        }

        public AcidInfo build(Path path)
        {
            // 1. For the given path fetch bucket ID
            // 2. Fetch list of all the original files which have same bucket ID
            // 3. Build AcidInfo
            if (pathToBucketIdMap.containsKey(path)) {
                long bucketId = pathToBucketIdMap.get(path);
                if (bucketIdToOriginalFilesMap.containsKey(bucketId)) {
                    return new AcidInfo(this.deleteDeltaLocations, Optional.of(bucketIdToOriginalFilesMap.get(bucketId).build()),
                            Optional.of(bucketId));
                }
            }
            return new AcidInfo(this.deleteDeltaLocations, this.originalFileLocations, this.bucketId);
        }
    }
}
