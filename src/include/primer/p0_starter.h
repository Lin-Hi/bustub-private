//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {
using std::move;
using std::unique_ptr;

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    this->rows_ = rows;
    this->cols_ = cols;
    this->linear_ = new T[rows * cols];
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual auto GetRowCount() const -> int = 0;

  /** @return The number of columns in the matrix */
  virtual auto GetColumnCount() const -> int = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual auto GetElement(int i, int j) const -> T = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] this->linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; ++i) {
      data_[i] = &(this->linear_[cols * i]);
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  auto GetRowCount() const -> int override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  auto GetColumnCount() const -> int override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  auto GetElement(int i, int j) const -> T override {
    if (i < 0 || i >= this->rows_) {
      LOG_INFO("File %s, Line %d, Function %s: Row number is out of range.\n", __FILE__, __LINE__, __FUNCTION__);
      throw Exception(ExceptionType::OUT_OF_RANGE, "");
    }

    if (j < 0 || j >= this->cols_) {
      LOG_INFO("File %s, Line %d, Function %s: Column number is out of range.\n", __FILE__, __LINE__, __FUNCTION__);
      throw Exception(ExceptionType::OUT_OF_RANGE, "");
    }

    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || i >= this->rows_) {
      LOG_INFO("File %s, Line %d, Function %s: Row number is out of range.\n", __FILE__, __LINE__, __FUNCTION__);
      throw Exception(ExceptionType::OUT_OF_RANGE, "");
    }

    if (j < 0 || j >= this->cols_) {
      LOG_INFO("File %s, Line %d, Function %s: Column number is out of range.\n", __FILE__, __LINE__, __FUNCTION__);
      throw Exception(ExceptionType::OUT_OF_RANGE, "");
    }

    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (static_cast<int>(source.size()) != GetRowCount() * GetColumnCount()) {
      LOG_INFO("File %s, Line %d, Function %s: Vector does not contain the required number of elements.\n", __FILE__,
               __LINE__, __FUNCTION__);
      throw Exception(ExceptionType::OUT_OF_RANGE, "");
    }

    for (int i = 0; i < this->rows_; ++i) {
      for (int j = 0; j < this->cols_; ++j) {
        data_[i][j] = source[i * this->cols_ + j];
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override { delete[] this->data_; };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static auto Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation
    if (matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixB->GetColumnCount()) {
      LOG_INFO("File %s, Line %d, Function %s: Dimensions mismatch for input matrices while adding.\n", __FILE__,
               __LINE__, __FUNCTION__);
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    unique_ptr<RowMatrix<T>> ans(new RowMatrix<T>(matrixA->GetRowCount(), matrixA->GetColumnCount()));
    for (int i = 0; i < matrixA->GetRowCount(); ++i) {
      for (int j = 0; j < matrixA->GetColumnCount(); ++j) {
        ans->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
      }
    }
    return ans;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static auto Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation
    if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {
      LOG_INFO("File %s, Line %d, Function %s: Dimensions mismatch for matrices in multiplication.\n", __FILE__,
               __LINE__, __FUNCTION__);
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    unique_ptr<RowMatrix<T>> ans(new RowMatrix<T>(matrixA->GetRowCount(), matrixB->GetColumnCount()));
    T ele;
    for (int i = 0; i < matrixA->GetRowCount(); ++i) {
      for (int j = 0; j < matrixB->GetColumnCount(); ++j) {
        ele = 0;
        for (int k = 0; k < matrixA->GetColumnCount(); ++k) {
          ele += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        }
        ans->SetElement(i, j, ele);
      }
    }
    return ans;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static auto GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB, const RowMatrix<T> *matrixC)
      -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation

    int ar = matrixA->GetRowCount();
    int br = matrixB->GetRowCount();
    int cr = matrixC->GetRowCount();
    int ac = matrixA->GetColumnCount();
    int bc = matrixB->GetColumnCount();
    int cc = matrixC->GetColumnCount();

    if (ac != br || (cr != ar || cc != bc)) {
      LOG_INFO("File %s, Line %d, Function %s: Dimensions mismatch for matrices in GEMM.\n", __FILE__, __LINE__,
               __FUNCTION__);
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    unique_ptr<RowMatrix<T>> ans;
    ans = move(Add(Multiply(matrixA, matrixB).get(), matrixC));
    return ans;
  }
};
}  // namespace bustub
