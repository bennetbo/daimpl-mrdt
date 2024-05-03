package mrdt

import org.scalatest.funsuite.AnyFunSuite

class IntegrationTests extends AnyFunSuite {
  class TodoItem(val id: Int, val text: String) extends Ordered[TodoItem] {
    override def compare(that: TodoItem): Int = this.id - that.id
  }

  test("todo_list") {
    val buyMilk = new TodoItem(1, "Buy milk")
    val walkDog = new TodoItem(2, "Walk the dog")
    val doLaundry = new TodoItem(3, "Do laundry")
    val cleanHouse = new TodoItem(4, "Clean the house")
    val cookDinner = new TodoItem(5, "Cook dinner")

    val state = List.of(
      buyMilk,
      walkDog,
      doLaundry
    )

    val replica1 = state.clone()
    replica1.insert(0, cleanHouse)

    val replica2 = state.clone()
    replica2.removeAt(1)
    replica2.insert(0, cookDinner)

    val mergedState = state.merge(replica1, replica2)

    assert(mergedState.size === 4)
    assert(mergedState.indexOf(cleanHouse) == Some(0))
    assert(mergedState.indexOf(cookDinner) == Some(1))
    assert(mergedState.indexOf(buyMilk) == Some(2))
    assert(mergedState.indexOf(doLaundry) == Some(3))
  }
}
