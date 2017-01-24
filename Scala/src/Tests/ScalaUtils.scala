package src.Tests

import cascading.tuple.hadoop.TupleSerializationProps
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by ramaharjan on 1/24/17.
  */
object ScalaUtils {

  //deleting files if exists
  def deleteResource(path: String) {
    val fs = FileSystem.get(getHadoopConf())
    fs.delete(new Path(path), true);
  }

  //defining hadoop configuration for compatibility with cascading project
  def getHadoopConf(): Configuration = {
    val hadoopConf = new Configuration();
    hadoopConf.set(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS, "cascading.tuple.hadoop.TupleSerialization,org.apache.hadoop.io.serializer.WritableSerialization")
    hadoopConf.set("textinputformat.record.delimiter", "^*~")
    hadoopConf
  }
}
