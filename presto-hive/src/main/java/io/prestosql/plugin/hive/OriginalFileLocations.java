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
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Stores original files related information.
 * To calculate the correct starting row ID of an original file, OriginalFilesRegistry needs originalFiles list.
 */
public class OriginalFileLocations
{
    private final String partitionLocation;
    private final List<OriginalFileInfo> originalFiles;

    @JsonCreator
    public OriginalFileLocations(@JsonProperty("partitionLocation") String partitionLocation,
                                 @JsonProperty("originalFiles") List<OriginalFileInfo> originalFiles)
    {
        this.partitionLocation = requireNonNull(partitionLocation);
        this.originalFiles = ImmutableList.copyOf(requireNonNull(originalFiles));
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<OriginalFileInfo> getOriginalFiles()
    {
        return originalFiles;
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
        OriginalFileLocations that = (OriginalFileLocations) o;
        return Objects.equals(partitionLocation, that.partitionLocation) &&
                Objects.equals(originalFiles, that.originalFiles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionLocation, originalFiles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionLocation", partitionLocation)
                .add("originalFiles", originalFiles)
                .toString();
    }

    public static class OriginalFileInfo
    {
        private final String path;
        private final long fileSize;

        @JsonCreator
        public OriginalFileInfo(@JsonProperty("path") String path,
                                @JsonProperty("fileSize") long fileSize)
        {
            this.path = path;
            this.fileSize = fileSize;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public long getFileSize()
        {
            return fileSize;
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
            OriginalFileInfo that = (OriginalFileInfo) o;
            return fileSize == that.fileSize &&
                    Objects.equals(path, that.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path, fileSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("path", path)
                    .add("fileSize", fileSize)
                    .toString();
        }
    }

    public static class Builder
    {
        private final String partitionLocation;
        private final ImmutableList.Builder originalFileInfoBuilder;

        public Builder(Path partitionPath)
        {
            partitionLocation = partitionPath.toString();
            originalFileInfoBuilder = new ImmutableList.Builder();
        }

        public void addOriginalFileInfo(String path, long size)
        {
            originalFileInfoBuilder.add(new OriginalFileInfo(path, size));
        }

        public OriginalFileLocations build()
        {
            return new OriginalFileLocations(partitionLocation, originalFileInfoBuilder.build());
        }
    }
}
