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

package io.prestosql.plugin.hive.orc.acid;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LazyBlockEncoding;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AcidBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(io.prestosql.spi.block.LazyBlock.class).instanceSize();

    private final ValidPositions validPositions;

    private final Block delegate;
    private final int deletegatePositionCount;

    public AcidBlock(int delegatePositionCount, Block delegate, ValidPositions validPositions)
    {
        this.validPositions = requireNonNull(validPositions, "validPositions is null");
        this.delegate = requireNonNull(delegate, "delegate block is null");

        this.deletegatePositionCount = delegatePositionCount;
    }

    @Override
    public int getPositionCount()
    {
        return validPositions.getPositionCount();
    }

    @Override
    public int getSliceLength(int position)
    {
        return delegate.getSliceLength(validPositions.getPosition(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.getByte(validPositions.getPosition(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.getShort(validPositions.getPosition(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.getInt(validPositions.getPosition(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.getLong(validPositions.getPosition(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.getSlice(validPositions.getPosition(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return delegate.getObject(validPositions.getPosition(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return delegate.bytesEqual(validPositions.getPosition(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return delegate.bytesCompare(validPositions.getPosition(position),
                offset,
                length,
                otherSlice,
                otherOffset,
                otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        delegate.writeBytesTo(validPositions.getPosition(position), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        delegate.writePositionTo(validPositions.getPosition(position), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return delegate.equals(validPositions.getPosition(position),
                offset,
                otherBlock,
                otherPosition,
                otherOffset,
                length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkState(offset == 0, "Offset has to be zero but found " + offset);
        return delegate.hash(validPositions.getPosition(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return delegate.compareTo(validPositions.getPosition(leftPosition),
                leftOffset,
                leftLength,
                rightBlock,
                rightPosition,
                rightOffset,
                rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return delegate.getSingleValueBlock(validPositions.getPosition(position));
    }

    @Override
    public long getSizeInBytes()
    {
        long totalSize = delegate.getSizeInBytes();

        // assume all rows to be similar size
        return totalSize * validPositions.getPositionCount() / deletegatePositionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        boolean[] positions = new boolean[position + length];
        while (position != length) {
            positions[position] = true;
        }
        return getPositionsSizeInBytes(positions);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        int lastRealPosition = validPositions.getPosition(positions.length - 1);
        boolean[] realPositions = new boolean[lastRealPosition + 1];

        for (int idx = 0; idx < positions.length; idx++) {
            if (positions[idx]) {
                realPositions[validPositions.getPosition(idx)] = true;
            }
        }
        return delegate.getPositionsSizeInBytes(realPositions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + delegate.getRetainedSizeInBytes() + validPositions.getPositionCount();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        delegate.retainedBytesForEachPart(consumer);
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return LazyBlockEncoding.NAME;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        return delegate.getPositions(getRealPositions(positions), offset, length);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        return delegate.copyPositions(getRealPositions(positions), offset, length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return copyRegion(positionOffset, length);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int[] positions = getValidPositions(positionOffset, length);
        return copyPositions(positions, 0, length);
    }

    @Override
    public boolean mayHaveNull()
    {
        return delegate.mayHaveNull();
    }

    @Override
    public boolean isNull(int position)
    {
        return delegate.isNull(validPositions.getPosition(position));
    }

    private int[] getValidPositions(int positionOffset, int length)
    {
        int[] positions = new int[length];
        for (int position = 0; position < length; position++) {
            positions[position] = validPositions.getPosition(positionOffset + position);
        }
        return positions;
    }

    private int[] getRealPositions(int[] positions)
    {
        int[] realPositions = new int[positions.length];
        int idx = 0;
        for (int position : positions) {
            realPositions[idx++] = validPositions.getPosition(position);
        }
        return realPositions;
    }
}
