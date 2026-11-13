package distributed.sparkvector

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object HighlyVariableGenes {
  // Seurat v1-style HVG selection (scanpy flavor="seurat"). The input matrix is
  // a log1p-normalized count matrix; per gene the dispersion is
  // log(variance / mean) (logVMR) computed on the un-logged counts, genes are
  // binned by log1p(mean), and the dispersion is z-score normalized within each
  // bin. Returns the gene indices with the top_k normalized dispersions.
  def fit(matrix: RDD[Vector], rows: Long, n_bins: Int, top_k: Int) : List[Int] = {
    val counts = matrix.map(v => Vectors.dense(v.toArray.map(math.expm1)))
    val means = genes_mean(counts, rows)
    val variance = genes_variance(counts, means, rows)
    val disp = dispersion(variance, means).toArray
    val bins = bin(Vectors.dense(means.toArray.map(math.log1p)), n_bins)
    val norm_disps = new Array[Double](means.size)
    bins.foreach(bin => {
      val d = bin_dispersion_mean(bin, disp)
      val std = bin_std(bin, disp, d)
      bin_normalized_dispersions(bin, disp, d, std)
        .foreach{case (i, v) => norm_disps(i) = v}
    })
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
    // Sample variance: mean of (v - mean(v)) squared with an n - 1 denominator.
    val mean_values = means.toArray
    val variance = matrix
      .map(b => {
        val b_values = b.toArray
        Vectors.dense(b_values.zipWithIndex.map{case (v, i) =>
          val diff = v - mean_values(i)
          diff * diff
        })
      })
      .reduce{case (a, b) =>
        Vectors.dense(a.toArray.zip(b.toArray).map{case (x, y) => x + y})
      }
    val denom = math.max(len - 1, 1).toDouble
    Vectors.dense(variance.toArray.map(_ / denom))
  }

  def dispersion(variance: Vector, means: Vector) : Vector = {
    // logVMR = log(variance / mean). A zero mean is clipped to a small value;
    // a zero dispersion (constant gene) becomes NaN.
    Vectors.dense(variance.toArray.zip(means.toArray).map{case (v, m) =>
      val mu = if (m == 0.0) 1e-12 else m
      val d = v / mu
      if (d == 0.0) Double.NaN else math.log(d)
    })
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
    val vals = bin.map(dispersions(_)).filterNot(_.isNaN)
    if (vals.isEmpty) Double.NaN else vals.sum / vals.length
  }

  def bin_std(bin: List[Int], dispersions: Array[Double], mean: Double): Double = {
    // Sample standard deviation of the bin dispersions (n - 1 denominator);
    // NaN when the bin has fewer than two finite dispersions
    val vals = bin.map(dispersions(_)).filterNot(_.isNaN)
    if (vals.length < 2) Double.NaN
    else math.sqrt(vals.map(v => math.pow(v - mean, 2)).sum / (vals.length - 1))
  }

  def bin_normalized_dispersions(bin: List[Int], dispersions: Array[Double], disp_mean_bin: Double, std_bin: Double) : List[(Int, Double)] = {
    // Keep the gene index together with its normalized dispersion. A bin with a
    // single finite dispersion has undefined std; as in Seurat its normalized
    // dispersion is set to 1.
    if (std_bin.isNaN) {
      bin.map(i => (i, if (dispersions(i).isNaN) Double.NaN else 1.0))
    } else {
      bin.map(i => (i, (dispersions(i) - disp_mean_bin) / std_bin))
    }
  }

  def rank_genes(normalized_dispersions: Array[Double], top_k: Int): List[Int] = {
    // The array is indexed by gene id, so the returned ranks are gene indices.
    // NaN dispersions sort last and drop out of the top_k.
    val ranks = normalized_dispersions.zipWithIndex.sortBy(-_._1).toList
    if (top_k > 0) {
      ranks.take(top_k).map(_._2)
    } else {
      ranks.map(_._2)
    }
  }

}
