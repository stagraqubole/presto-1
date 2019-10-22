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

import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.core.CachingFileSystem;
import io.airlift.log.Logger;
import io.prestosql.spi.classloader.ThreadContextClassLoader;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

// Shared across catalogs in order to have singleton Rubix services
public class RubixServices
{
    private static final Logger log = Logger.get(RubixServices.class);

    private BookKeeper bookKeeper;

    public void setBookKeeper(BookKeeper bookKeeper)
    {
        checkState(this.bookKeeper == null, "Bookkeeper is already set");
        this.bookKeeper = bookKeeper;
    }

    public Optional<BookKeeper> getBookKeeper()
    {
        return Optional.ofNullable(bookKeeper);
    }

    // Register each catalog's CachingFileSystem
    public void initializeBookKeeper(ClassLoader catalogClassLoader)
    {
        checkState(bookKeeper != null, "Bookkeeper is not set");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(catalogClassLoader)) {
            CachingFileSystem.setLocalBookKeeper(bookKeeper);
            log.info("Rubix initialized successfully");
        }
    }
}
