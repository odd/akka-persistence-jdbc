package akka.persistence.jdbc.util

import akka.persistence.PersistentRepr
import akka.persistence.jdbc.common.PluginConfig
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization

trait EncodeDecode {
  def serialization: Serialization
  def cfg: PluginConfig

  object Journal extends JournalProvider {
    private[this] val underlying: JournalProvider = {
      if (cfg.base64Format) new Base64JournalProvider
      else if (cfg.jsonFormat) new JsonJournalProvider
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
      else if (cfg.jsonFormat) new JsonSnapshotProvider
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

  class JsonJournalProvider extends JournalProvider {
    import org.json4s._
    import org.json4s.native.Serialization
    import org.json4s.native.Serialization.{read, write}
    implicit val formats = Serialization.formats(NoTypeHints)

    def toString(msg: PersistentRepr): String = write(msg)
    def toBytes(msg: PersistentRepr): Array[Byte] = toString(msg).getBytes("UTF-8")

    def fromString(str: String): PersistentRepr = read(str)
    def fromBytes(bytes: Array[Byte]): PersistentRepr = fromString(new String(bytes, "UTF-8"))
  }

  class JsonSnapshotProvider extends SnapshotProvider {
    import org.json4s._
    import org.json4s.native.Serialization
    import org.json4s.native.Serialization.{read, write}
    implicit val formats = Serialization.formats(NoTypeHints)
    def toString(msg: Snapshot): String = write(msg)
    def toBytes(msg: Snapshot): Array[Byte] = toString(msg).getBytes("UTF-8")

    def fromString(str: String): Snapshot = read(str)
    def fromBytes(bytes: Array[Byte]): Snapshot = fromString(new String(bytes, "UTF-8"))
  }
}
