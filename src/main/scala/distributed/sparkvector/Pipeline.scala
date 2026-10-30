package distributed.sparkvector

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed._
import distributed.sparkvector.HighlyVariableGenes

object Pipeline {
  def run(config: Map[String, String]) : Unit = {
    val mode = if (config("mode") == "sparklocal") "local[*]" else "yarn"
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

  def read_dense_csv(path: String)(sc: SparkContext) : RDD[Vector] = {
    sc.textFile(path).map(row => Vectors.dense(row.split(",").map(_.toDouble)))
  }

  def read_sparse_csv(path: String)(ss: SparkSession) : RDD[Vector] = {
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
        // Spark requires the indices of a sparse vector to be strictly increasing
        // val data = iter.toArray.sortBy(_._1).unzip
        Vectors.sparse(27998, iter.toSeq)}
      }
  }

  def genes_count(mat: RDD[Vector]) : Vector = {
    mat.reduce{case (a, b) =>
      Vectors.dense(a.toArray.zip(b.toArray).map{case (x, y) => x + y})}
  }

  def cells_count(mat: RDD[Vector]) : RDD[Double] = {
    mat.map(_.toArray.sum)
  }

  def median_value(cells_sums: Array[Double]) : Double = {
    val mid = cells_sums.length / 2
    if (cells_sums.length % 2 == 0) {
      (cells_sums(mid - 1) + cells_sums(mid)) / 2
    } else {
      cells_sums(mid / 2)
    }
  }

  def normalize_total(mat: RDD[Vector], target_row_sum: Double, row_sums_opt : Option[RDD[Double]] = None) : RDD[Vector] = {
    val rowSums = row_sums_opt match {
      case None => cells_count(mat)
      case Some(sums) => sums
    }
    mat.zip(rowSums).map{case (row, sum) => {
      val rate = target_row_sum / sum
      Vectors.dense(row.toArray.map(_ * rate))
    }}
  }

  def highest_expr_genes(mat: RDD[Vector], top: Int, row_sums_opt : Option[RDD[Double]] = None) : Array[(Double, Int)] = {
    val norm = normalize_total(mat, 100)
    genes_count(norm).toArray.zipWithIndex.sortBy{case (v, index) => -v}.take(top)
   }

  def filter_cells_mask(mat: RDD[Vector], min_genes: Int): RDD[Boolean] = {
    mat.map(_.numNonzeros >= min_genes)
  }

  def filter_genes_mask(mat: RDD[Vector], min_cells: Int) : Array[Boolean] = {
    mat.map(_
      .toArray
      .map{case v => if (v != 0) 1 else 0})
      .reduce(
        (a, b) => a.zip(b).map{case (v1, v2) => v1 + v2})
      .map(_ >= min_cells)
      .toArray
  }

  def remove_rows_with_mask(mat: RDD[Vector], mask: RDD[Boolean]) : RDD[Vector] = {
    mat.zip(mask).filter(_._2).map(_._1)
  }

  def remove_columns_with_mask(mat: RDD[Vector], mask: Array[Boolean]): RDD[Vector] = {
    mat.map(row =>
      row match {
        case r : SparseVector => {
          val vals = r.indices.zip(r.values).filter{case (i, _) => mask(i)}.unzip
          Vectors.sparse(mask.length, vals._1, vals._2)
        }
        case _ => {
          Vectors.dense(row.toArray.zip(mask).filter{case (_, b) => b == true}.map(_._1))
                    }
      })
  }

  def log1p_matrix(mat: RDD[Vector]) : RDD[Vector] = {
    mat.map(row => Vectors.dense(row.toArray.map(math.log1p)))
  }

  def keep_highly_variable(mat: RDD[Vector], ranks: List[Int], cols: Int) : RowMatrix = {
    new RowMatrix(mat.map(row => {
      // Vectors have been limited to a 1000 length, and we only keep the most valuable, so keeping them dense
      // might not only be feasible, but also save space.
      val values = row.toArray
      linalg.Vectors.dense(ranks.map(values(_)).toArray[Double])
    }))
  }
}
