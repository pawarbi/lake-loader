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

package utils

import scala.io.Source
import scala.util.{Try, Using}

object FileUtils {

  def loadDoubles(filePath: String): List[Double] = {
    Using.resource(Source.fromFile(filePath)) { source =>
      source.getLines().flatMap(line => Try(line.toDouble).toOption).toList
    }
  }

  def loadLongs(filePath: String): List[Long] = {
    Using.resource(Source.fromFile(filePath)) { source =>
      source.getLines().flatMap(line => Try(line.toLong).toOption).toList
    }
  }
}
