package zio.cache

import zio.Chunk
import zio.prelude.{NonEmptyList}
import zio.schema._
import zio.schema.ast.NodePath

import scala.annotation.tailrec

sealed trait Query[S, A] { self =>
  def apply(value: S): Boolean

  def and[A2](that: Query[S, A2]): Query[S, (A, A2)] = Query.And(self, that)

  def &[A2](that: Query[S, A2]): Query[S, (A, A2)] = Query.And(self, that)

  def or[A2](that: Query[S, A2]): Query[S, (A, A2)] = Query.Or(self, that)

  def |[A2](that: Query[S, A2]): Query[S, (A, A2)] = Query.Or(self, that)
}

object Query {
  case class SelectError(path: NodePath, message: String) extends RuntimeException

  case class GreaterThan[S, A](selector: Selector[S, A], that: A) extends Query[S, A] {
    override def apply(value: S): Boolean =
      selector(value).map { v =>
        selector.fieldSchema.ordering.compare(v, that) > 0
      }.getOrElse(false)
  }

  case class LessThan[S, A](selector: Selector[S, A], that: A) extends Query[S, A] {
    override def apply(value: S): Boolean =
      selector(value).map { v =>
        selector.fieldSchema.ordering.compare(v, that) < 0
      }.getOrElse(false)
  }

  case class EqualTo[S, A](selector: Selector[S, A], that: A) extends Query[S, A] {
    override def apply(value: S): Boolean =
      selector(value).map(_ == that).getOrElse(false)
  }

  case class Contains[S,A](iterator: Iterator[S,A], that: A) extends Query[S,A]

  case class IsA[S,A](discriminator: Discriminator[S,A]) extends Query[S,A]

  case class And[S, A1, A2](predicate1: Query[S, A1], predicate2: Query[S, A2]) extends Query[S, (A1, A2)] {
    override def apply(value: S): Boolean = predicate1(value) && predicate2(value)
  }

  case class Or[S, A1, A2](predicate1: Query[S, A1], predicate2: Query[S, A2]) extends Query[S, (A1, A2)] {
    override def apply(value: S): Boolean = predicate1(value) || predicate2(value)
  }

  object QueryAccessorBuilder extends AccessorBuilder {
    override type Lens[S, A]      = Selector[S, A]
    override type Prism[S, A]     = Discriminator[S, A]
    override type Traversal[S, A] = Iterator[S, A]

    override def makeLens[S, A](product: Schema.Record[S], term: Schema.Field[A]): Selector[S, A] =
      Selector(product, NonEmptyList(term.label), term.schema)

    override def makePrism[S, A](sum: Schema.Enum[S], term: Schema.Case[A, S]): Discriminator[S, A] =
      Discriminator(sum, term.id, term.codec)

    override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Iterator[S, A] =
      Iterator(collection)
  }

  case class Cursor[S,A](whole: Schema[S], part: Schema[A], ops: NonEmptyList[CursorOp]) {
    def get(whole: S): Either[String, Option[A]] = ???
    def update(whole: S, newPart: A): Either[String, S] = ???
  }

  sealed trait CursorOp

  case class Iterator[S, A](collection: Schema.Collection[S, A]) extends CursorOp { self =>
    def apply(value: S, index: Int): Either[String, A] = ???

    def contains(that: A): Contains[S,A] = Contains(self,that)

    def |=(that: A): Contains[S,A] = contains(that)
  }

  case class Selector[S, A](product: Schema[S], fieldPath: NonEmptyList[String], fieldSchema: Schema[A])
      extends CursorOp {
    self =>
    def >(that: A): GreaterThan[S, A] = GreaterThan(self, that)
    def <(that: A): LessThan[S, A]    = LessThan(self, that)
    def =:=(that: A): EqualTo[S, A]   = EqualTo(self, that)

    def compose[A1](that: Selector[A, A1]): Selector[S, A1] =
      self.copy(fieldPath = self.fieldPath ++ that.fieldPath, fieldSchema = that.fieldSchema)

    def discriminate[A1](that: Discriminator[A,A1]): Selector[S,A1] =
      self.copy(fieldPath = self.fieldPath ++ NonEmptyList.single(that.label), fieldSchema = that.subtype)

    def /[A1](that: Selector[A, A1]): Selector[S, A1] = compose(that)

    def /?[A1](that: Discriminator[A,A1]): Selector[S,A1] = discriminate(that)

    def apply(value: S): Either[String, A] = {
      @tailrec
      def go(rec: DynamicValue.Record, path: NonEmptyList[String]): Either[String, A] =
        path.peelNonEmpty match {
          case (next, _) if !rec.values.contains(next) => Left(s"Field $next does not exist")
          case (next, None)                            => fieldSchema.fromDynamic(rec.values(next))
          case (next, Some(rest)) =>
            rec.values(next) match {
              case rec0: DynamicValue.Record => go(rec0, rest)
              case _                         => Left(s"Field $next is not a record type")
            }
        }

      product.toDynamic(value) match {
        case rec @ DynamicValue.Record(_) => go(rec, fieldPath)
        case _                            => Left(s"Cannot select field from non-record type")
      }
    }

    def erased: Selector[_, _] = self.asInstanceOf[Selector[Any, Any]]
  }

  case class Discriminator[S, A](enum: Schema.Enum[S], label: String, subtype: Schema[A]) extends CursorOp {



    def apply(value: S): Either[String, Option[A]] = enum.toDynamic(value) match {
      case DynamicValue.Enumeration((label0, value0)) if label0 == label =>
        subtype.fromDynamic(value0).map(Some(_))
      case _ => Left(s"Cannot cast subtype on non-enum type")
    }
  }
}
