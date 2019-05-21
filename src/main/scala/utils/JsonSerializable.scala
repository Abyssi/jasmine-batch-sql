package utils

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

trait JsonSerializable {
  override def toString: String = this.toJsonString

  def toJsonString: String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    write(this)
  }
}
