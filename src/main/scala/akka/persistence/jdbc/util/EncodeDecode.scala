package akka.persistence.jdbc.util

import java.util.concurrent.TimeUnit

import akka.actor.{ActorNotFound, ActorSystem, ActorRef}
import akka.persistence.{PersistentImpl, PersistentRepr}
import akka.persistence.jdbc.common.PluginConfig
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization
import akka.util.Timeout
import net.liftweb.json.CustomSerializer
import net.liftweb.json.JsonAST.JString
import net.liftweb.json._
import net.liftweb.json.ext.JodaTimeSerializers
import org.joda.time.{DateTime, LocalDateTime}

//import org.json4s.JsonAST.JString
//import org.json4s.native.Serialization
//import org.json4s.{CustomSerializer, FullTypeHints}
import net.liftweb.json.JsonAST.JString
import net.liftweb.json.{Serialization => JsonSerialization, DefaultFormats, Formats, CustomSerializer, FullTypeHints}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.FiniteDuration

trait EncodeDecode {
  def serialization: Serialization
  def cfg: PluginConfig

  object Journal extends JournalProvider {
    private[this] val underlying: JournalProvider = {
      if (cfg.base64Format) new Base64JournalProvider
      else if (cfg.jsonFormat) new JsonJournalProvider(serialization.system)
      else throw new IllegalStateException("Message format undefined")
    }

    def toBytes(msg: PersistentRepr): Array[Byte] = underlying.toBytes(msg)
    def toString(msg: PersistentRepr): String = underlying.toString(msg)

    def fromBytes(bytes: Array[Byte]): PersistentRepr = underlying.fromBytes(bytes)
    def fromString(str: String): PersistentRepr = underlying.fromString(str)
  }

  object Snapshot extends SnapshotProvider {
    private[this] val underlying: SnapshotProvider = {
      if (cfg.base64Format) new Base64SnapshotProvider
      else if (cfg.jsonFormat) new JsonSnapshotProvider(serialization.system)
      else throw new IllegalStateException("Message format undefined")
    }

    def toBytes(msg: Snapshot): Array[Byte] = underlying.toBytes(msg)
    def toString(msg: Snapshot): String = underlying.toString(msg)

    def fromBytes(bytes: Array[Byte]): Snapshot = underlying.fromBytes(bytes)
    def fromString(str: String): Snapshot = underlying.fromString(str)
  }

  trait JournalProvider {
    def toBytes(msg: PersistentRepr): Array[Byte]
    def toString(msg: PersistentRepr): String

    def fromBytes(bytes: Array[Byte]): PersistentRepr
    def fromString(str: String): PersistentRepr
  }

  trait SnapshotProvider {
    def toBytes(msg: Snapshot): Array[Byte]
    def toString(msg: Snapshot): String

    def fromBytes(bytes: Array[Byte]): Snapshot
    def fromString(str: String): Snapshot
  }

  class Base64JournalProvider extends JournalProvider {
    def toBytes(msg: PersistentRepr): Array[Byte] = serialization.serialize(msg).get
    def toString(msg: PersistentRepr): String = Base64.encodeString(toBytes(msg))

    def fromBytes(bytes: Array[Byte]): PersistentRepr = serialization.deserialize(bytes, classOf[PersistentRepr]).get
    def fromString(str: String): PersistentRepr = fromBytes(Base64.decodeBinary(str))
  }

  class Base64SnapshotProvider extends SnapshotProvider {
    def toBytes(msg: Snapshot): Array[Byte] = serialization.serialize(msg).get
    def toString(msg: Snapshot): String = Base64.encodeString(toBytes(msg))

    def fromBytes(bytes: Array[Byte]): Snapshot = serialization.deserialize(bytes, classOf[Snapshot]).get
    def fromString(str: String): Snapshot = fromBytes(Base64.decodeBinary(str))
  }

  /*
  class AutoFullTypeHints extends FullTypeHints(Nil) {
    override def containsHint_?(clazz: Class[_]): Boolean = classOf[Product].isAssignableFrom(clazz)
  }
  */

  private val timeout = Timeout(10, TimeUnit.SECONDS).duration

  class ActorRefSerializer(system: ActorSystem) extends CustomSerializer[ActorRef]({ formats => ({
      /*
      case JString(address) => Await.result(system.actorSelection(address).resolveOne(timeout).recoverWith {
        case t: ActorNotFound => Future.successful(ActorRef.noSender)
      }(system.dispatcher), timeout)
      */
      case JString(address) => system.actorFor(address)
    }, {
      case ref: ActorRef => JString(ref.path.toSerializationFormat)
     })
  })
  
  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => (
    {
      case JString(s) => new DateTime(s).toLocalDateTime
      case JNull => null
    },
    {
      case d: LocalDateTime => JString(format.dateFormat.format(d.toDate))
    }
  ))

  trait JsonCapable[Repr <: AnyRef] {
    def system: ActorSystem
    implicit def manifest: Manifest[Repr]
    implicit val formats = (new Formats {
      override val typeHintFieldName: String = "@class"
      val dateFormat = DefaultFormats.lossless.dateFormat
        override val typeHints = new FullTypeHints(List(classOf[Any]))
      } +
      new ActorRefSerializer(system) +
      LocalDateTimeSerializer) ++
      JodaTimeSerializers.all
    def toString(msg: Repr): String = JsonSerialization.write(msg)
    def toBytes(msg: Repr): Array[Byte] = toString(msg).getBytes("UTF-8")
    def fromString(str: String): Repr = JsonSerialization.read[Repr](str)
    def fromBytes(bytes: Array[Byte]): Repr = fromString(new String(bytes, "UTF-8"))
  }

  class JsonJournalProvider(val system: ActorSystem) extends JournalProvider with JsonCapable[PersistentRepr] {
    override implicit def manifest = Manifest.classType[PersistentRepr](classOf[PersistentImpl])
  }
  /*{
     def toString(msg: PersistentRepr): String = JsonSerialization.write(msg)
    def toBytes(msg: PersistentRepr): Array[Byte] = toString(msg).getBytes("UTF-8")
    def fromString(str: String): PersistentRepr = JsonSerialization.read[PersistentImpl](str)
    def fromBytes(bytes: Array[Byte]): PersistentRepr = fromString(new String(bytes, "UTF-8"))
  }
 */

  class JsonSnapshotProvider(val system: ActorSystem) extends SnapshotProvider with JsonCapable[Snapshot] {
    override implicit def manifest = Manifest.classType[Snapshot](classOf[Snapshot])
  }
  /*{
    def toString(msg: Snapshot): String = write(msg)
    def toBytes(msg: Snapshot): Array[Byte] = toString(msg).getBytes("UTF-8")
    def fromString(str: String): Snapshot = read(str)
    def fromBytes(bytes: Array[Byte]): Snapshot = fromString(new String(bytes, "UTF-8"))
  }
  */
}
