package edu.berkeley.veloxms.client

import org.scalatest._

class RequestGeneratorSpec extends FlatSpec with Matchers {

  "SamplerWithoutReplacement" should "run out of values" in {

    val sampler = new SamplerWithoutReplacement(3)
    val user = 1
    sampler.nextItem(user) should be ('defined)
    sampler.nextItem(user) should be ('defined)
    sampler.nextItem(user) should be ('defined)
    sampler.nextItem(user) should be (None)
  }

  it should "not repeat values" in {

    val sampler = new SamplerWithoutReplacement(3)
    val user = 1L
    val found = (0L until 3).map(_ => sampler.nextItem(user))
    found should contain theSameElementsAs List(Some(0), Some(1), Some(2))
  }





}
