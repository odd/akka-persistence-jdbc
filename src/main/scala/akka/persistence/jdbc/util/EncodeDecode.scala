package akka.persistence.jdbc.util

import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import akka.serialization.Serialization

trait EncodeDecode {
  def serialization: Serialization

  object Journal extends JournalProvider {
    private[this] val underlying: JournalProvider = {
      new Base64JournalProvider
    }

    def toBytes(msg: PersistentRepr): Array[Byte] = underlying.toBytes(msg)
    def toString(msg: PersistentRepr): String = underlying.toString(msg)

    def fromBytes(bytes: Array[Byte]): PersistentRepr = underlying.fromBytes(bytes)
    def fromString(str: String): PersistentRepr = underlying.fromString(str)
  }

  object Snapshot extends Snapshot {
    private[this] val underlying: SnapshotProvider = {
      new Base64SnapshotProvider
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
}
