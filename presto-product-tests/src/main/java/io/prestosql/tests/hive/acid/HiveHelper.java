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

import java.sql.SQLException;

import static io.prestosql.tests.utils.QueryExecutors.onHive;

public final class HiveHelper
{
    private HiveHelper()
    {}

    public static boolean canRunAcidTests()
            throws SQLException
    {
        return onHive().getConnection().getMetaData().getDatabaseMajorVersion() >= 3;
    }
}
