package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.Address
import akka.cluster.UniqueAddress

class ORMultiMapSpec extends WordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka.tcp", "Sys", "localhost", 2551), 1)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2)

  "A ORMultiMap" must {

    "be able to add entries" in {
      val m = ORMultiMap(ORMap()).addBinding(node1, "a", "A").addBinding(node1, "b", "B")
      val a = m.entries("a")
      a.value should be(Set("A"))
      val b = m.entries("b")
      b.value should be(Set("B"))

      val m2 = m.addBinding(node1, "a", "C")
      val a2 = m2.entries("a")
      a2.value should be(Set("A", "C"))
    }

    "be able to remove entry" in {
      val m = ORMultiMap(ORMap()).addBinding(node1, "a", "A").addBinding(node1, "b", "B").removeBinding(node1, "a", "A")
      m.entries.keySet should not contain ("a")
      m.entries.keySet should contain("b")
    }

    "be able to have its entries correctly merged with another ORMultiMap with other entries" in {
      val m1 = ORMultiMap(ORMap()).addBinding(node1, "a", "A").addBinding(node1, "b", "B")
      val m2 = ORMultiMap(ORMap()).addBinding(node2, "c", "C")

      // merge both ways
      val merged1 = m1 merge m2
      merged1.entries.keySet should contain("a")
      merged1.entries.keySet should contain("b")
      merged1.entries.keySet should contain("c")

      val merged2 = m2 merge m1
      merged2.entries.keySet should contain("a")
      merged2.entries.keySet should contain("b")
      merged2.entries.keySet should contain("c")
    }

    "be able to have its entries correctly merged with another ORMultiMap with overlapping entries" in {
      val m1 = ORMultiMap(ORMap())
        .addBinding(node1, "a", "A1")
        .addBinding(node1, "b", "B1")
        .removeBinding(node1, "a", "A1")
        .addBinding(node1, "d", "D1")
      val m2 = ORMultiMap(ORMap())
        .addBinding(node2, "c", "C2")
        .addBinding(node2, "a", "A2")
        .addBinding(node2, "b", "B2")
        .removeBinding(node2, "b", "B2")
        .addBinding(node2, "d", "D2")

      // merge both ways
      val merged1 = m1 merge m2
      merged1.entries.keySet should contain("a")
      val a1 = merged1.entries("a")
      a1.value should be(Set("A2"))
      merged1.entries.keySet should contain("b")
      val b1 = merged1.entries("b")
      b1.value should be(Set("B1"))
      merged1.entries.keySet should contain("c")
      merged1.entries.keySet should contain("d")
      val d1 = merged1.entries("d")
      d1.value should be(Set("D1", "D2"))

      val merged2 = m2 merge m1
      merged2.entries.keySet should contain("a")
      val a2 = merged1.entries("a")
      a2.value should be(Set("A2"))
      merged2.entries.keySet should contain("b")
      val b2 = merged2.entries("b")
      b2.value should be(Set("B1"))
      merged2.entries.keySet should contain("c")
      merged2.entries.keySet should contain("d")
      val d2 = merged2.entries("d")
      d2.value should be(Set("D1", "D2"))
    }

    "be able to update a case class entries with the same identity after merging" in {
      case class A(k: String, v: Boolean) {
        override def equals(o: Any) = o match {
          case that: A => that.k.equals(this.k)
          case _       => false
        }
        override def hashCode = k.hashCode()
      }

      val m1 = ORMultiMap(ORMap()).addBinding(node1, "a", A(k = "A", v = false))
      val m2 = ORMultiMap(ORMap()).addBinding(node2, "a", A(k = "A", v = true))

      val merged1 = m1 merge m2
      val a1 = merged1.entries("a")
      a1.value should be(Set(A(k = "A", v = true)))

      val merged2 = m2 merge m1
      val a2 = merged1.entries("a")
      a2.value should be(Set(A(k = "A", v = true)))
    }

  }
}