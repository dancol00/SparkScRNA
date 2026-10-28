package distributed.sparkbreeze

import org.apache.spark.rdd.RDD
import breeze.linalg._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed._
import breeze.numerics._
import distributed.sparkbreeze.HighlyVariableGenes

object Pipeline {
  def run(config: Map[String, String]) : Unit = {
    val mode = if (config("mode") == "local") "local[*]" else "yarn"
    val appName = "ScRNAPipeline"
    val sparkConf = new SparkConf().set("spark.driver.maxResultSize", "4g")
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .master(mode)
      .getOrCreate()
    val ss = spark
    import ss.implicits._
    val sc = spark.sparkContext


    var real_start = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    val input = config("type") match {
      case "sparse" => read_sparse_csv(config("input"))(ss)
      case _ => read_dense_csv(config("input"))(sc)
    }
    var elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for read_csv: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val genes = genes_count(input)
    val cells_rdd = cells_count(input)
    val cells = cells_rdd.collect()
    val median = median_value(cells)
    val heg = highest_expr_genes(input, 20, Some(cells_rdd))
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for phase 1: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val kr = filter_cells_mask(input, 200)
    val kc = filter_genes_mask(input, 3)
    val filtered_matrix_t = remove_rows_with_mask(input, kr)
    val filtered_matrix = remove_columns_with_mask(filtered_matrix_t, kc)
    val rows = kr.filter(_ == true).count()
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for phase 2: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val norm_mat = normalize_total(filtered_matrix, 10000)
    val log_mat = log1p_matrix(norm_mat)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for phase 3: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val ranks = HighlyVariableGenes.fit(log_mat, rows, 20, 1000)
    val hvg = keep_highly_variable(log_mat, ranks, 1000)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for phase 4: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val pca = hvg.computePrincipalComponents(50)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for phase 5: ${elapsed / 1000.0}")

    spark.stop()
    elapsed = System.currentTimeMillis() - real_start
    println(s"Done in ${elapsed / 1000.0}s.")
  }

  def read_dense_csv(path: String)(sc: SparkContext) : RDD[Vector[Double]] = {
    sc.textFile(path).map(row => row.split(",").map(_.toDouble)).map(DenseVector(_))
  }

  def read_sparse_csv(path: String)(ss: SparkSession) : RDD[Vector[Double]] = {
    val f = ss
      .read
      .csv(path)
      .rdd
      .map(row => {
        // Trasforma i dati da (data, col, row) a (row, col, data)
        (row.getString(2).trim.toInt, row.getString(1).trim.toInt, row.getString(0).trim.toDouble)
      })
    // RDD contiene (data, column, row)
    val grouped_by_row = f
      .groupBy{ case (row, _, _) => row }
      .map{ case (row, iter) => (row, iter.map{ case (row, col, data) => (col, data)})}
    grouped_by_row
      .map{case (_, iter) => {
        val data = iter.toArray.unzip
        new SparseVector(data._1, data._2, 27998)}
      }
  }

  def genes_count(mat: RDD[Vector[Double]]) : DenseVector[Double] = {
    mat.reduce{case (a, b) => a + b}.toDenseVector
  }

  def cells_count(mat: RDD[Vector[Double]]) : RDD[Double] = {
    mat.map(sum(_))
  }

  def median_value(cells_sums: Array[Double]) : Double = {
    val mid = cells_sums.length / 2
    if (cells_sums.length % 2 == 0) {
      (cells_sums(mid - 1) + cells_sums(mid)) / 2
    } else {
      cells_sums(mid / 2)
    }
  }

  def normalize_total(mat: RDD[Vector[Double]], target_row_sum: Double, row_sums_opt : Option[RDD[Double]] = None) : RDD[Vector[Double]] = {
    val rowSums = row_sums_opt match {
      case None => cells_count(mat)
      case Some(sums) => sums
    }
    mat.zip(rowSums).map{case (row, sum) => {
      val rate = target_row_sum / sum
      row * rate
    }}
  }

  def highest_expr_genes(mat: RDD[Vector[Double]], top: Int, row_sums_opt : Option[RDD[Double]] = None) : Array[(Double, Int)] = {
    val norm = normalize_total(mat, 100)
    genes_count(norm).data.zipWithIndex.sortBy{case (v, index) => -v}.take(top)
   }

  def filter_cells_mask(mat: RDD[Vector[Double]], min_genes: Int): RDD[Boolean] = {
    mat.map(_.valuesIterator.count(_ != 0) >= min_genes)
  }

  def filter_genes_mask(mat: RDD[Vector[Double]], min_cells: Int) : Array[Boolean] = {
//     mat.map(_.valuesIterator.map{case v => if (v > 0) 1 else 0}.toArray).map(DenseVector[Int](_)).reduce{case (a, b) => a + b}.data.zipWithIndex.filter{case (v, i) => v >= min_cells}.map(_._2)
    mat.map(_
      .valuesIterator
      .map{case v => if (v != 0) 1 else 0}
      .toArray)
      .reduce(
        (a, b) => a.zip(b).map{case (v1, v2) => v1 + v2})
      .map(_ >= min_cells)
      .toArray
  }

  def remove_rows_with_mask(mat: RDD[Vector[Double]], mask: RDD[Boolean]) : RDD[Vector[Double]] = {
    mat.zip(mask).filter(_._2).map(_._1)
  }

  def remove_columns_with_mask(mat: RDD[Vector[Double]], mask: Array[Boolean]): RDD[Vector[Double]] = {
    mat.map(row =>
      row match {
        case r : SparseVector[Double] => {
//          val vals = r.activeIterator.collect{case r if mask.contains(r._1) => r}.toArray.unzip
          val vals = row.iterator.zip(mask).filter{case (_, b) => b == true}.map(_._1).toArray.unzip
          new SparseVector[Double](vals._1, vals._2, mask.length)
        }
        case _ => {
//          val vals = row.valueIterator.collect{case r if mask.contains(r._1) => r._2}.toArray
          val vals = row.valuesIterator.zip(mask).filter{case (_, b) => b == true}.map(_._1).toArray
          new DenseVector[Double](vals)
                    }
      })
  }

  def log1p_matrix(mat: RDD[Vector[Double]]) : RDD[Vector[Double]] = {
    mat.map(log1p(_))
  }

  def keep_highly_variable(mat: RDD[Vector[Double]], ranks: List[Int], cols: Int) : RowMatrix = {
    new RowMatrix(mat.map(row => {
      // Vectors have been limited to a 1000 length, and we only keep the most valuable, so keeping them dense
      // might not only be feasible, but also save space.
      linalg.Vectors.dense(ranks.map(row(_)).toArray[Double])
    }))
  }
}
