package local

import os._
import breeze.linalg._
import breeze.numerics._
import breeze.stats._

object Pipeline {
  def run(config: Map[String, String]) : Unit = {
    var start = System.currentTimeMillis()
    var input = readCsv_toArray(config("input"), (29606, 36753))
//    println(s"Input size: ${input.length}x${input(0).length}")
    var elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for read_csv: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val genes = genes_count(input)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for genes_count: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val cells = cells_count(input)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for cells_count: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val median = median_value(cells)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for median_value: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val heg = highest_expr_genes(input, cells, 20)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for highest_expr_genes: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val kr = filter_cells_mask(input, 200)    
    elapsed = System.currentTimeMillis() - start
    println(s"Found ${kr.count(_ == false)} cells to remove")
    println(s"Time elapsed for filter_cells_mask: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val kc = filter_genes_mask(input, 3)    
    elapsed = System.currentTimeMillis() - start
    println(s"Found ${kc.count(_ == false)} genes to remove")
    println(s"Time elapsed for filter_genes_mask: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val filtered_matrix_t = remove_rows_with_mask(input, kr)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for remove_rows_with_mask: ${elapsed / 1000.0}")    

    start = System.currentTimeMillis()
    val filtered_matrix = remove_columns_with_mask(filtered_matrix_t, kc)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for remove_columns_with_mask: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val norm_mat = normalize_total(filtered_matrix, cells, 10000)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for normalize_total: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val log_mat = log1p_matrix(norm_mat)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for log1p_matrix: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val ranks = HighlyVariableGenes.fit(log_mat, 20, 1000)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for HighlyVariableGenes: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val hvg = keep_highly_variable(log_mat, ranks, log_mat.length, 1000)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for keep_highly_variable: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val cov = covmat(hvg)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for covmat: ${elapsed / 1000.0}")

    start = System.currentTimeMillis()
    val scores = new PCA(hvg, cov)
    elapsed = System.currentTimeMillis() - start
    println(s"Time elapsed for PCA: ${elapsed / 1000.0}")
  }



  def readCsv_toArray(filename: String, shape: (Int, Int)) : Array[DenseVector[Double]] = {
    val path = os.Path(filename)
    val reader = os.read.lines.stream(path)
    val result = reader.map(_.split(',').map(_.toDouble)).map(DenseVector(_)).toArray
    result
  }

  def readCsv_toDenseMatrix(filename: String, shape: (Int, Int)): DenseMatrix[Double] = {
    val path = os.Path(filename)
    val reader = os.read.lines.stream(path)
    val data: Array[Double] = reader.flatMap(_.split(',').map(_.toDouble)).toArray
    new DenseMatrix[Double](shape._1, shape._2, data, 0, shape._2, isTranspose = true)
  }

  def genes_count(mat: Array[DenseVector[Double]]) : DenseVector[Double] = {
    mat.reduce[DenseVector[Double]]{case (a, b) => a + b}
  }

  def genes_count(mat: DenseMatrix[Double]) : DenseVector[Double] = {
    sum(mat, Axis._0).t
  }

  def genes_count(mat: Matrix[Double]) : DenseVector[Double] = {
    var sums = DenseVector.zeros[Double](mat.cols)
    mat.activeIterator.foreach{case ((row, col), v) => sums(col) += v}
    sums
  }

  def cells_count(mat: Array[DenseVector[Double]]) : Array[Double] = {
    mat.map(sum(_))
  }

  def cells_count(mat: DenseMatrix[Double]) : Array[Double] = {
    sum(mat, Axis._1).data
  }

  def cells_count(mat: Matrix[Double]) : Array[Double] = {
    var sums = DenseVector.zeros[Double](mat.rows)
    mat.activeIterator.foreach{case ((row, col), v) => sums(row) += v}
    sums.data
  }

  def median_value(cells_sums: Array[Double]) : Double = {
    val mid = cells_sums.length / 2
    if (cells_sums.length % 2 == 0) {
      (cells_sums(mid - 1) + cells_sums(mid)) / 2
    } else {
      cells_sums(mid / 2)
    }
  }

  def normalize_total(mat: Array[DenseVector[Double]], rowSums: Array[Double], target_row_sum: Double) : Array[DenseVector[Double]] = {
    mat.zip(rowSums).map{case (row, rowSum) => {
      val rate = target_row_sum / rowSum
      row *= rate
    }}
  }
/*
  def normalize_total(mat: DenseMatrix[Double], target_row_sum: Double) : DenseMatrix[Double] = {
    val rowSums = cells_count(mat).data
    // Mutate in place, in order to avoid unnecessary copies
    mat.foreachPair{case ((row, col),v) => {
      val rate = target_row_sum / rowSums(row)
      mat(row, col) = v * rate
      // DenseMatrix is col-major, has a stride and offset, so finding the actual index is more obscure.
      // mat.data(mat.linearIndex(row, col)) = v * rate
    }}
    mat
  }
 */
  def normalize_total(mat: Matrix[Double], rowSums: Array[Double], target_row_sum: Double) : Matrix[Double] = {
    mat.foreachPair{case ((row, col),v) => {
      val rate = target_row_sum / rowSums(row)
      mat(row, col) = v * rate
    }}
    mat
  }

  def highest_expr_genes(mat: Array[DenseVector[Double]], rowSums: Array[Double], top: Int) : Array[(Double, Int)] = {
    val norm = normalize_total(mat, rowSums, 100)
    genes_count(norm).data.zipWithIndex.sortBy{case (v, index) => -v}.take(top)//.map{ case (a, b) => (b, a) }
  }

  def highest_expr_genes(mat: DenseMatrix[Double], rowSums: Array[Double], top: Int) : Array[(Double, Int)] = {
    val norm = normalize_total(mat, rowSums,  100)
    genes_count(norm).data.zipWithIndex.sortBy{case (v, index) => -v}.take(top)//.map{ case (a, b) => (b, a) }
  }

  def filter_cells_mask(mat: Array[DenseVector[Double]], min_genes: Int): Array[Boolean] = {
    mat.map(_.data.count(_ > 0) >= min_genes)
  }

  def filter_cells_mask(mat: Matrix[Double], min_genes: Int): Array[Boolean] = {
    var count = DenseVector.zeros[Int](mat.cols)
    mat.activeIterator.foreach{case ((row, col), v) => if (v > 0) count.data(col) += 1}
    count.data.map(_ >= min_genes)
  }

  def filter_genes_mask(mat: Array[DenseVector[Double]], min_cells: Int) : Array[Boolean] = {
    mat.map(_.data.map(v => if (v > 0) 1 else 0)).map(DenseVector[Int]).reduce[DenseVector[Int]]{case (a, b) => a + b}.data.map(_ >= min_cells)
  }

  def filter_genes_mask(mat: Matrix[Double], min_cells: Int) : Array[Boolean] = {
    var count = DenseVector.zeros[Int](mat.rows)
    mat.activeIterator.foreach{case ((row, col), v) => if (v > 0) count.data(row) += 1}
    count.data.map(_ >= min_cells)
  }

  /// Remove the rows where the array shows a False value
  def remove_rows_with_mask(mat: Array[DenseVector[Double]], mask: Array[Boolean]) : Array[DenseVector[Double]] = {
    mat.zip(mask).filter(_._2).map(_._1)
  }

  def remove_columns_with_mask(mat: Array[DenseVector[Double]], mask: Array[Boolean]): Array[DenseVector[Double]] = {
    mat.map(_.data.zip(mask).filter(_._2).map(_._1)).map(DenseVector[Double])
  }

  // Alternative: create a view that ignores the removed values, while keeping everything intact.
  // Requires more space overall, but avoids copies
  def remove_rows_with_mask(mat: DenseMatrix[Double], mask: Array[Boolean]): DenseMatrix[Double] = {
    val delRows = mask.zipWithIndex.collect { case (false, i) => i }
    mat.delete(delRows, Axis._0)
    mat
  }

  def remove_rows_with_mask(mat: CSCMatrix[Double], mask: Array[Boolean]): CSCMatrix[Double] = {
    val keepRows = mask.zipWithIndex.collect { case (true, i) => i }
    val rowMap = keepRows.zipWithIndex.toMap
    val builder = new CSCMatrix.Builder[Double](keepRows.length, mat.cols)
    mat.activeIterator.foreach { case ((row, col), v) =>
      if (rowMap.contains(row)) builder.add(rowMap(row), col, v)
    }
    builder.result()
  }

  def remove_columns_with_mask(mat: DenseMatrix[Double], mask: Array[Boolean]): DenseMatrix[Double] = {
    val delCols = mask.zipWithIndex.collect { case (false, i) => i }
    mat.delete(delCols, Axis._1)
    mat
  }

  def remove_columns_with_mask(mat: CSCMatrix[Double], mask: Array[Boolean]): CSCMatrix[Double] = {
    val keepCols = mask.zipWithIndex.collect { case (true, i) => i }
    val colMap = keepCols.zipWithIndex.toMap  // old row -> new row
    val builder = new CSCMatrix.Builder[Double](mat.rows, keepCols.length)
    mat.activeIterator.foreach { case ((row, col), v) =>
      if (colMap.contains(col)) builder.add(row, colMap(col), v)
    }
    builder.result()
  }

  def remove_with_mask(mat: Array[DenseVector[Double]], row_mask: Array[Boolean], col_mask: Array[Boolean]): Array[DenseVector[Double]] = {
    mat.zip(row_mask).filter(_._2).map(_._1.data.zip(col_mask).filter(_._2).map(_._1)).map(DenseVector[Double])
  }

  def filter_cells(mat: Array[DenseVector[Double]], min_genes: Int) : Array[DenseVector[Double]] = {
    mat.filter(_.data.count(_ > 0) >= min_genes)
  }
  /*
  def filter_genes(mat: Array[DenseVector[Double]], min_cells: Int) : Array[DenseVector[Double]] = {
    mat.map(_.data.map(v => if (v > 0) 1 else 0)).map(DenseVector[Int]).reduce[DenseVector[Int]]{case (a, b) => a + b}.data.map(_ > min_cells)
   }
   */

  def log1p_matrix(mat: Array[DenseVector[Double]]) : Array[DenseVector[Double]] = {
    mat.map(log1p.inPlace(_))
  }

  def log1p_matrix(mat: Matrix[Double]) : Matrix[Double] = {
    mat.iterator.foreach{case ((row, col), v) => mat(row, col) = log1p(v)}
    mat
  }

  def log1p_matrix(mat: DenseMatrix[Double]) : DenseMatrix[Double] = {
    log1p(mat)
  }

  def keep_highly_variable(mat: Array[DenseVector[Double]], ranks: List[Int], rows: Int, cols: Int) : DenseMatrix[Double] = {
    val data = mat.flatMap(vec => ranks.map(vec.data(_)))
    new DenseMatrix[Double](
      rows,
      cols,
      data,
      0,
      cols,
      isTranspose = true
    )
  }

  def pca(data: DenseMatrix[Double], components: Int): DenseMatrix[Double] = {
    def mean(v: Vector[Double]): Double = v.valuesIterator.sum / v.size

    def zeroMean(m: DenseMatrix[Double]): DenseMatrix[Double] = {
      for (c <- 0 until m.cols) {
        val col = m(::, c)
        val colMean = mean(col)
        col -= colMean
      }
      m
    }
    val d = zeroMean(data)
    val svd.SVD(_, _, v) = svd(d.t)
    val model = v(0 until components, ::) //top 'components' eigenvectors
    val filter = model.t * model
    filter *= d
  }
}

