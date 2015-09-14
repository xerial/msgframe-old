/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xerial.msgframe.core

import java.sql.DriverManager


/**
 *
 */
class MsgFrameTest extends MsgFrameSpec {

  def withResource[U, In <: AutoCloseable](in: In)(f: In => U): U = {
    try {
      f(in)
    }
    finally {
      if (in != null)
        in.close
    }
  }

  before {
    Class.forName("org.sqlite.JDBC")
  }

  "MsgFrame" should {
    "read data from JDBC results" in {
      withResource(DriverManager.getConnection("jdbc:sqlite::memory:")){ conn =>
        withResource(conn.createStatement()) { st =>
          st.execute("CREATE TABLE sample (id integer, name string, date date)")
          st.execute("INSERT INTO sample values(1, 'leo', '2015-01-23')")
          st.execute("INSERT INTO sample values(2, 'yui', '2015-12-31')")
        }

        withResource(conn.createStatement()) { st =>
          withResource(st.executeQuery("select * from sample")) { rs =>
            val frame = MsgFrame.fromSQL(rs)
            info(frame)

            frame.numColumns shouldBe 3
            frame.numRows shouldBe 2
            frame.colNames shouldBe Seq("id", "name", "date")

          }
        }
      }

    }

  }
}
