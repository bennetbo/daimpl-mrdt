package mrdt

import org.scalatest.funsuite.AnyFunSuite

class IntegrationTests extends AnyFunSuite {
  class TodoItem(val id: Int, val text: String) extends Ordered[TodoItem] {
    override def compare(that: TodoItem): Int = this.id - that.id
    override def equals(x: Any): Boolean = x match {
      case that: TodoItem => this.id == that.id
      case _              => false
    }
    override def hashCode(): Int = this.id
  }

  test("todo_list") {
    val state = MrdtList.of(
      TodoItem(1, "Buy milk"),
      TodoItem(2, "Walk the dog"),
      TodoItem(3, "Do laundry")
    )

    val replica1 = state.insert(0, TodoItem(4, "Clean the house"))

    val replica2 =
      state.removeAt(1)._2.insert(0, TodoItem(5, "Cook dinner"))

    val mergedState = state.merge(replica1, replica2)

    assert(mergedState.size === 4)
    assert(mergedState.indexOf(TodoItem(4, "Clean the house")) == Some(0))
    assert(mergedState.indexOf(TodoItem(5, "Cook dinner")) == Some(1))
    assert(mergedState.indexOf(TodoItem(1, "Buy milk")) == Some(2))
    assert(mergedState.indexOf(TodoItem(3, "Do laundry")) == Some(3))
  }
}
