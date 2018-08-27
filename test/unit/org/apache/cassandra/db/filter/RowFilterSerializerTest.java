/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link org.apache.cassandra.db.filter.RowFilter.Serializer}
 */
public class RowFilterSerializerTest extends CQLTester
{

    @Test
    public void inExpressionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (key int, c1 int, c2 int, s1 text static, PRIMARY KEY ((key, c1), c2))");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 11, 1, 's1')");

        List<ByteBuffer> bufferList = new ArrayList<>();
        bufferList.add(ByteBuffer.allocate(Integer.BYTES).putInt(1));

        ColumnMetadata keyMetadata = getCurrentColumnFamilyStore().metadata().getColumn(new ColumnIdentifier("key", true));
        RowFilter.InExpression inExpression = new RowFilter.InExpression(keyMetadata, Operator.IN, bufferList);
        RowFilter rowFilter = RowFilter.create();
        rowFilter.addIn(keyMetadata, Operator.IN, bufferList);

        ByteBuf byteBuf = Unpooled.buffer(1024, 1024);
        try (ByteBufDataOutputPlus out = new ByteBufDataOutputPlus(byteBuf))
        {
            RowFilter.serializer.serialize(rowFilter, out, MessagingService.VERSION_30);
            try (ByteBufDataInputPlus in = new ByteBufDataInputPlus(out.buffer()))
            {
                RowFilter desearializedFilter = RowFilter.serializer.deserialize(in, MessagingService.VERSION_30, getCurrentColumnFamilyStore().metadata());
                assertEquals(1, desearializedFilter.expressions.size());
                assertTrue(desearializedFilter.expressions.get(0) instanceof RowFilter.InExpression);
                rowFilter.expressions.get(0).validate();
            }
        }
    }
}
