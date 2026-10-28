package distributed.sparkvector

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object HighlyVariableGenes {
  def fit(matrix: RDD[Vector], rows: Long, n_bins: Int, top_k: Int) : List[Int] = {
    val means = genes_mean(matrix, rows)
    val variance = genes_variance(matrix, means, rows)
    val disp = dispersion(variance).toArray
    val bins = bin(means, n_bins)
    val norm_disps = bins
      .map(bin => {
        val d = bin_dispersion_mean(bin, disp)
        val std = bin_std(bin, disp, d)
        bin_normalized_dispersions(bin, disp, d, std)
      }
      ).flatten
    rank_genes(norm_disps, top_k)
  }

  def genes_mean(matrix: RDD[Vector], len: Long) : Vector = {
    // len is the number of rows
    val means = matrix.reduce{case (a, b) =>
      Vectors.dense(a.toArray.zip(b.toArray).map{case (x, y) => x + y})
    }
    Vectors.dense(means.toArray.map(_ / len.toDouble))
  }

  def genes_variance(matrix: RDD[Vector], means: Vector, len: Long) : Vector = {
    // Mean of (v - mean(v)) squared
    val mean_values = means.toArray
    val variance = matrix.fold(Vectors.zeros(means.size)){(a, b) =>
      val b_values = b.toArray
      Vectors.dense(a.toArray.zipWithIndex.map{case (acc, i) =>
        val diff = b_values(i) - mean_values(i)
        acc + diff * diff
      })
    }
    Vectors.dense(variance.toArray.map(_ / len.toDouble))
  }

  def dispersion(variance: Vector) : Vector = {
    Vectors.dense(variance.toArray.map(math.sqrt))
  }

  def bin(means: Vector, n_bins: Int) : List[List[Int]] = {
    var data: List[List[Int]] = List()

    val mean_values = means.toArray
    val min_v = mean_values.min
    val max_v = mean_values.max + 0.1
    val bin_range = (max_v - min_v) / n_bins.toDouble

    var current_bin: List[Int] = List()
    var current_min = min_v
    var current_max = min_v + bin_range

    val sorted = mean_values
      .zipWithIndex
      .sortBy(_._1)
      .foreach{case (v, i) =>
        // Since the list is sorted, checking the min is not needed.
        if (v >= current_max) {
          data :+= current_bin
          current_bin = List()
          while (v >= current_max) {
            current_min += bin_range
            current_max += bin_range
          }
        }
        current_bin :+= i
      }
    data :+= current_bin
    data
  }

  def bin_dispersion_mean(bin: List[Int], dispersions: Array[Double]): Double = {
    math.sqrt(bin.map(dispersions(_)).sum / bin.length)
  }

  def bin_std(bin: List[Int], dispersions: Array[Double], mean: Double): Double = {
    math.sqrt(bin.map(i => dispersions(i) - mean).sum / bin.length - 1)
  }

  def bin_normalized_dispersions(bin: List[Int], dispersions: Array[Double], disp_mean_bin: Double, std_bin: Double) : List[Double] = {
    bin.map(i => (dispersions(i) + disp_mean_bin) / std_bin)
  }

  def rank_genes(normalized_dispersions: List[Double], top_k: Int): List[Int] = {
    val ranks = normalized_dispersions.zipWithIndex.sortBy(-_._1)
    if (top_k > 0) {
      ranks.take(top_k).map(_._2)
    } else {
      ranks.map(_._2)
    }
  }

}
