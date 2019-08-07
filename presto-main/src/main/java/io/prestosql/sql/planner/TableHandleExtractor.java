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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.Optional;

public class TableHandleExtractor
{
    public Multimap<CatalogName, ConnectorTableHandle> extractTableHandles(PlanNode root)
    {
        return root.accept(new Visitor(), ImmutableMultimap.builder()).build();
    }

    private static class Visitor
            extends PlanVisitor<ImmutableMultimap.Builder, ImmutableMultimap.Builder>
    {
        @Override
        protected ImmutableMultimap.Builder visitPlan(PlanNode node, ImmutableMultimap.Builder builder)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, builder);
            }

            return builder;
        }

        @Override
        public ImmutableMultimap.Builder visitTableScan(TableScanNode node, ImmutableMultimap.Builder builder)
        {
            builder.put(node.getTable().getCatalogName(), node.getTable().getConnectorHandle());
            return visitPlan(node, builder);
        }

        @Override
        public ImmutableMultimap.Builder visitDelete(DeleteNode node, ImmutableMultimap.Builder builder)
        {
            TableHandle tableHandle = node.getTarget().getHandle();
            builder.put(tableHandle.getCatalogName(), tableHandle.getConnectorHandle());
            return visitPlan(node, builder);
        }

        @Override
        public ImmutableMultimap.Builder visitTableDelete(TableDeleteNode node, ImmutableMultimap.Builder builder)
        {
            TableHandle tableHandle = node.getTarget();
            builder.put(tableHandle.getCatalogName(), tableHandle.getConnectorHandle());
            return visitPlan(node, builder);
        }

        @Override
        public ImmutableMultimap.Builder visitTableFinish(TableFinishNode node, ImmutableMultimap.Builder builder)
        {
            Optional<TableHandle> tableHandle = node.getTarget().getTableHandle();
            tableHandle.map(handle -> builder.put(handle.getCatalogName(), handle.getConnectorHandle()));
            return visitPlan(node, builder);
        }
    }
}
