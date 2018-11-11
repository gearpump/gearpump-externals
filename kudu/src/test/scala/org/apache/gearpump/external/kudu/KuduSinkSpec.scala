/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.external.kudu

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.kudu.KuduSink.{KuduWriter, KuduWriterFactory}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.kudu.client._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KuduSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {


  property("KuduSink should invoke KuduWriter for writing message to Kudu") {

    implicit val system = ActorSystem
    val kuduWriter = mock[KuduWriter]
    val kuduWriterFactory = mock[KuduWriterFactory]

    val userConfig = UserConfig.empty
    val tableName = "kudu"

    when(kuduWriterFactory.getKuduWriter(userConfig, tableName))
      .thenReturn(kuduWriter)

    val kuduSink = new KuduSink(userConfig, tableName, kuduWriterFactory)

    kuduSink.open(mock[TaskContext])

    val value = ("key", "value")
    val message = Message(value)
    kuduSink.write(message)
    verify(kuduWriter, atLeastOnce()).put(message.value)

    kuduSink.close()
    verify(kuduWriter).close()
  }

  property("KuduWriter should insert a row successfully") {

    val table = mock[KuduTable]
    val kuduClient = mock[KuduClient]
    val tableName = "kudu"

    when(kuduClient.openTable(tableName)).thenReturn(table)
  }
}