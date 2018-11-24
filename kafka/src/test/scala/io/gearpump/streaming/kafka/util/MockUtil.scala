package io.gearpump.streaming.kafka.util

import akka.actor.ActorSystem
import org.apache.gearpump.streaming.task.{TaskContext, TaskId}
import org.mockito.{ArgumentMatcher, Matchers, Mockito}

object MockUtil {

  lazy val system: ActorSystem = ActorSystem("mockUtil")

  def mockTaskContext: TaskContext = {
    val context = Mockito.mock(classOf[TaskContext])
    Mockito.when(context.system).thenReturn(system)
    Mockito.when(context.parallelism).thenReturn(1)
    Mockito.when(context.taskId).thenReturn(TaskId(0, 0))
    context
  }

  def argMatch[T](func: T => Boolean): T = {
    Matchers.argThat(new ArgumentMatcher[T] {
      override def matches(param: Any): Boolean = {
        val message = param.asInstanceOf[T]
        func(message)
      }
    })
  }
}