package edu.berkeley.veloxms.pipelines

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

@SerialVersionUID(1l)
trait PipelineStage[A, B] extends Serializable

class Pipeline[A, B : ClassTag, C : ClassTag](initStage: PipelineStage[A, B], nextStage: PipelineStage[B, C]) extends Estimator[A, C] {
  def andThen[D : ClassTag](stage: PipelineStage[C, D]): Pipeline[A, C, D] = {
    new Pipeline[A, C, D](this, stage)
  }

  override def fit(in: RDD[A]): TransformerChain[A, B, C] = {
    val firstTransformer = initStage match {
      case estimator: Estimator[A, B] => estimator.fit(in)
      case transformer: Transformer[A, B] => transformer
      case _ => throw new IllegalArgumentException
    }

    val secondTransformer = nextStage match {
      case estimator: Estimator[B, C] => estimator.fit(in.mapPartitions(firstTransformer.transform))
      case transformer: Transformer[B, C] => transformer
      case _ => throw new IllegalArgumentException
    }

    new TransformerChain[A, B, C](firstTransformer, secondTransformer)
  }
}

object Pipeline {
  def Pipeline[A, B : ClassTag, C : ClassTag](a: PipelineStage[A, B], b: PipelineStage[B, C]) = new Pipeline[A, B, C](a, b)
  def Pipeline[A, B : ClassTag](a: PipelineStage[A, B]) = new Pipeline[A, B, B](a, new IdentityTransformer[B])
  def Pipeline[A : ClassTag]() = new Pipeline[A, A, A](new IdentityTransformer[A], new IdentityTransformer[A])
}

class TransformerChain[A, B, C](val a: Transformer[A, B], val b: Transformer[B, C]) extends Transformer[A, C] {
  override def transform(in: A): C = b.transform(a.transform(in))
  override def transform(in: Iterator[A]): Iterator[C] = b.transform(a.transform(in))
  def lastTransformer: Transformer[B, C] = b
}

class IdentityTransformer[T] extends Transformer[T, T] {
  override def transform(in: T): T = in
}

abstract class Transformer[A, B] extends PipelineStage[A, B] {
  def transform(in: A): B
  def transform(in: Iterator[A]): Iterator[B] = in.map(transform)
  def andThen[C](next: Transformer[B, C]) = new TransformerChain[A, B, C](this, next)
}

abstract class Estimator[A, B] extends PipelineStage[A, B] {
  def fit(in: RDD[A]): Transformer[A, B]
}