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

package ai.onehouse.lakeloader.utils

import java.util.UUID

object MathUtils {
  def genUniformDist(n: Int): List[Double] = {
    normalize(List.fill(n)(1)).toList
  }

  def genExponentialDist(lambda: Double, n: Int): List[Double] = {
    normalize((0 until n).map { i => Math.exp(-lambda * i) }).toList
  }

  def normalize(s: Seq[Double]): Seq[Double] = {
    val sum = s.sum
    s.map(_ / sum)
  }

  private def randomUUID(): String =
    UUID.randomUUID().toString

  def sampleFromCDF(cdf: List[Double], weight: Double): Int =
    cdf.indexWhere(d => weight <= d)

  def makeCDF(weights: List[Double]): List[Double] =
    weights.scanLeft(0.0)(_ + _).tail

  /**
   * Computes the number of inputs per bucket based on a zipf distributions. The returned
   * list is sorted in descending order, with the first bucket having the highest number of inputs,
   * and the final bucket having the lowest number of inputs.
   * We ensure each bucket has at least 1 entry, so the final output might exceed the number of inputs.
   * @param inputs
   * @param buckets
   * @param shape
   * @return
   */
  def zipfDistribution(inputs: Long, buckets: Int, shape: Double = 2.93): List[Int] = {
    val ranks = (1 to buckets).map(_.toDouble)
    val rawProbs = ranks.map(r => 1.0 / math.pow(r, shape))
    val sumProbs = rawProbs.sum
    val normalizedProbs = rawProbs.map(_ / sumProbs)

    // Calculate record count for each bucket
    val recordsPerBucket = normalizedProbs.map(p => Math.max(1, (p * inputs).toInt))

    // Sort from largest to smallest bucket
    val sortedRecords = recordsPerBucket.sorted(Ordering[Int].reverse)
    sortedRecords.toList
  }

}
