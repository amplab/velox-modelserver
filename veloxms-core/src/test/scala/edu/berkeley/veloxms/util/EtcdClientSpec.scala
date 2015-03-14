package edu.berkeley.veloxms.util

import org.scalatest.{FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory
import edu.berkeley.veloxms._
import scala.concurrent._
import ExecutionContext.Implicits.global
import dispatch.StatusCode

class EtcdClientSpec extends FlatSpec with Matchers with MockFactory  {

  val host = "127.0.0.1"
  val port = 4001
  val modelName = "testModel"

  val jsonKeyNotExists = jsonMapper.writeValueAsString(EtcdError("key doesn't exist", 100, 0, "message"))
  val jsonRandomError = jsonMapper.writeValueAsString(EtcdError("key doesn't exist", 104, 0, "message"))

  val jsonUnlocked = jsonMapper.writeValueAsString(
      EtcdResponse("read", NodeResponse(1, modelName, 1, Some(EtcdConstants.UNLOCKED), None, None), None))

  val jsonAlreadyLocked = jsonMapper.writeValueAsString(
      EtcdResponse("read", NodeResponse(1, modelName, 1, Some("otherNode"), None, None), None))

  val jsonLockedSuccess = jsonMapper.writeValueAsString(
      EtcdResponse(
                   "write",
                   NodeResponse(2, modelName, 2, Some(host), None, None),
                   Some(NodeResponse(1, modelName, 1, Some(EtcdConstants.UNLOCKED), None, None))))



  "EtcdClient" should "acquire retrain lock when lock does not exist" in {

    val dispatchStub = mock[DispatchUtil]
    inSequence {
      (dispatchStub.sendRequestAccept404 _).expects(*).returning( future {(404, jsonKeyNotExists)})
      // what sendRequest returns is unnecessary, as long as the request succeeds the lock was acquired
      (dispatchStub.sendRequest _).expects(*).returning( future { jsonUnlocked })
    }

    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (true)

  }

  it should "acquire retrain lock when lock is available" in {

    val dispatchStub = mock[DispatchUtil]
    inSequence {
      (dispatchStub.sendRequestAccept404 _).expects(*).returning( future { (200, jsonUnlocked) })
      // don't check return value right now
      (dispatchStub.sendRequest _).expects(*).returning( future { jsonLockedSuccess })
    }
    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (true)

  }

  it should "not acquire retrain lock when etcd error occurs" in {
    val dispatchStub = mock[DispatchUtil]
    (dispatchStub.sendRequestAccept404 _).expects(*).returning( future {(404, jsonRandomError)})
    // what sendRequest returns is unnecessary, as long as the request succeeds the lock was acquired
    // (dispatchStub.sendRequest _).expects(*).returning( future { jsonUnlocked })

    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (false)


  }

  it should "not acquire retrain lock when etcd non-404 error occurs" in {
    val dispatchStub = mock[DispatchUtil]
    (dispatchStub.sendRequestAccept404 _).expects(*).returning( future {(503, jsonRandomError)})
    // what sendRequest returns is unnecessary, as long as the request succeeds the lock was acquired
    // (dispatchStub.sendRequest _).expects(*).returning( future { jsonUnlocked })

    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (false)


  }

  it should "not acquire retrain lock when lock is already held" in {

    val dispatchStub = mock[DispatchUtil]
    // inSequence {
      (dispatchStub.sendRequestAccept404 _).expects(*).returning( future { (200, jsonAlreadyLocked) })
      // don't check return value right now
      // (dispatchStub.sendRequest _).expects(*).returning( future { jsonLockedSuccess })
    // }
    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (false)

  }

  it should "not acquire retrain lock when lock acquired between check and write" in {

    val dispatchStub = mock[DispatchUtil]
    inSequence {
      (dispatchStub.sendRequestAccept404 _).expects(*).returning( future { (200, jsonUnlocked) })
      // don't check return value right now
      (dispatchStub.sendRequest _).expects(*) throws StatusCode(404)
    }
    val etcdClient = new EtcdClient(host, port, host, dispatchStub)
    etcdClient.acquireRetrainLock(modelName) should be (false)

  }

}
