package distributed

import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._


object HighlyVariableGenes {

  def fit(matrix: RDD[Vector[Double]], rows: Int, n_bins: Int, top_k: Int) : List[Int] = {
    val means = genes_mean(matrix, rows)
    val variance = genes_variance(matrix, means, rows)
    val disp = dispersion(variance).data
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

  def genes_mean(matrix: RDD[Vector[Double]], len: Int) : DenseVector[Double] = {
    // len is the number of rows
    val means = matrix.reduce{case (a, b) => a + b} *:* (1 / len.toDouble)
    means.toDenseVector
  }

  def genes_variance(matrix: RDD[Vector[Double]], means: DenseVector[Double], len: Int) : DenseVector[Double] = {
    // Mean of (v - mean(v)) squared
    val variance = matrix.fold(DenseVector.zeros[Double](means.length)){(a, b) => a + ((b - means) ^:^ 2.0)} *:* (1 / len.toDouble)
    variance.toDenseVector
  }

  def dispersion(variance: DenseVector[Double]) : DenseVector[Double] = {
    sqrt(variance)
  }

  def bin(means: DenseVector[Double], n_bins: Int) : List[List[Int]] = {
    var data: List[List[Int]] = List()

    val min_v = min(means)
    val max_v = max(means) + 0.1
    val bin_range = (max_v - min_v) / n_bins.toDouble

    var current_bin: List[Int] = List()
    var current_min = min_v
    var current_max = min_v + bin_range

    val sorted = means
      .data
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
    sqrt(bin.map(dispersions(_)).sum / bin.length)
  }

  def bin_std(bin: List[Int], dispersions: Array[Double], mean: Double): Double = {
    sqrt(bin.map(i => dispersions(i) - mean).sum / bin.length - 1)
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
