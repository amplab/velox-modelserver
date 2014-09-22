package edu.berkeley.veloxms

class MatrixFactorizationModel[Long](
    numFeatures: Int,
    modelStorage: ModelStorage) extends Model {

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): Array[Double] = {
    modelStorage.get(data)
  }

}



