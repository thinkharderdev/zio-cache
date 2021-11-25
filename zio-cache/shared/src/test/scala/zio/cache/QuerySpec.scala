package zio.cache

import zio.random.Random
import zio.test._
import zio.test.environment.TestEnvironment
import zio.schema._

object QuerySpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Failure] = suite("QuerySpec")(
    suite("records")(
      testM("EqualTo") {
        check(User.gen) { user =>
          assertTrue(applyQuery(User.id =:= user.id, user))
          assertTrue(applyQuery(User.name =:= user.name, user))
          assertTrue(applyQuery(User.age =:= user.age, user))
        }
      },
      testM("GreaterThan") {
        check(User.gen) { user =>
          assertTrue(applyQuery(User.id > (user.id - 1), user))
          assertTrue(applyQuery(User.age > (user.age - 1), user))
        }
      },
      testM("LessThan") {
        check(User.gen) { user =>
          assertTrue(applyQuery(User.id < (user.id + 1), user))
          assertTrue(applyQuery(User.age < (user.age + 1), user))
        }
      },
      testM("And") {
        check(User.gen) { user =>
          assertTrue(applyQuery((User.id =:= user.id) & (User.name =:= user.name) & (User.age =:= user.age), user))
        }
      },
      testM("Or") {
        check(User.gen) { user =>
          assertTrue(
            applyQuery((User.id =:= user.id + 1) | (User.name =:= user.name) | (User.age =:= user.age + 1), user)
          )
        }
      }
    )
  )

  def applyQuery[S](query: Query[S, _], operand: S): Boolean = query(operand)

  case class User(id: Long, name: String, age: Int)

  object User {
    implicit val schema = DeriveSchema.gen[User]

    val (id, name, age) = schema.makeAccessors(Query.QueryAccessorBuilder)

    val gen: Gen[Random with Sized, User] =
      for {
        id   <- Gen.anyLong
        name <- Gen.anyString
        age  <- Gen.anyInt
      } yield User(id, name, age)
  }
  case class Session(id: String, user: User)

  object Session {
    implicit val schema = DeriveSchema.gen[Session]

    val (id, user) = schema.makeAccessors(Query.QueryAccessorBuilder)

    val gen: Gen[Random with Sized, Session] =
      for {
        id   <- Gen.anyString
        user <- User.gen
      } yield Session(id, user)
  }
}
