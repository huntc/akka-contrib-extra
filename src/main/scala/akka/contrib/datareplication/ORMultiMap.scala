package akka.contrib.datareplication

import akka.cluster.{ UniqueAddress, Cluster }

object ORMultiMap {

  val empty: ORMultiMap =
    new ORMultiMap(ORMap.empty)

  def apply(map: ORMap): ORMultiMap =
    new ORMultiMap(map)
}

/**
 * An immutable multi-map implementation for akka-data-replication.
 */
@SerialVersionUID(1L)
class ORMultiMap private (private[akka] val map: ORMap)
    extends ReplicatedData with RemovedNodePruning with Serializable {

  override type T = ORMultiMap

  override def merge(that: T): T =
    new ORMultiMap(map.merge(that.map))

  def entries: Map[String, ORSet] =
    map.entries.asInstanceOf[Map[String, ORSet]]

  def get(key: String): Option[ORSet] =
    map.get(key).asInstanceOf[Option[ORSet]]

  def +(entry: (String, ORSet))(implicit node: Cluster): ORMultiMap = {
    val (key, value) = entry
    put(node, key, value)
  }

  def put(node: Cluster, key: String, value: ORSet): ORMultiMap =
    put(node.selfUniqueAddress, key, value)

  private[akka] def put(node: UniqueAddress, key: String, value: ORSet): ORMultiMap =
    ORMultiMap(map.put(node, key, value))

  def -(key: String)(implicit node: Cluster): ORMultiMap =
    remove(node, key)

  def remove(node: Cluster, key: String): ORMultiMap =
    remove(node.selfUniqueAddress, key)

  private[akka] def remove(node: UniqueAddress, key: String): ORMultiMap =
    ORMultiMap(map.remove(node, key))

  def addBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    addBinding(cluster.selfUniqueAddress, key, element)

  private[akka] def addBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val values = updateOrInit(key, _.add(node, element), ORSet.empty.add(node, element))
    ORMultiMap(map.put(node, key, values))
  }

  def removeBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    removeBinding(cluster.selfUniqueAddress, key, element)

  private[akka] def removeBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val values = updateOrInit(key, _.remove(node, element), ORSet.empty)
    if (values.value.nonEmpty)
      ORMultiMap(map.put(node, key, values))
    else
      ORMultiMap(map.remove(node, key))
  }

  private def updateOrInit(key: String, update: ORSet => ORSet, init: => ORSet): ORSet =
    map.get(key).asInstanceOf[Option[ORSet]] match {
      case Some(values) => update(values)
      case None         => init
    }

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    map.needPruningFrom(removedNode)

  override def pruningCleanup(removedNode: UniqueAddress): T =
    new ORMultiMap(map.pruningCleanup(removedNode))

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    new ORMultiMap(map.prune(removedNode, collapseInto))
}
