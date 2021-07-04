object myapp extends App{

  import org.apache.hadoop.conf._
  import org.apache.hadoop.fs._
  import java.net.URI
  import org.apache.log4j.BasicConfigurator
  import java.io.BufferedInputStream
  import java.io.File
  import java.io.FileInputStream
  import java.io.InputStream
  import scala.collection.mutable.ArrayBuffer
  import org.apache.log4j.{Level, Logger}

  System.setProperty("hadoop.home.dir", "/")
  BasicConfigurator.configure()

  val conf = new Configuration()
  conf.set("dfs.replication", "1")
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  val rootLogger = Logger.getRootLogger
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  rootLogger.setLevel(Level.ERROR)


  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  // Get the first level files and directories under the directory
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = fileSystem.listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }
  // Get the first level file under the directory
  def getFiles(path: String): Array[String] = {
    getFilesAndDirs(path).filter(fileSystem.getFileStatus(_).isFile())
      .map(_.toString)
  }
  // Get the first level directory under the directory
  def getDirs(path: String): Array[String] = {
    getFilesAndDirs(path).filter(fileSystem.getFileStatus(_).isDirectory)
      .map(_.toString)
  }
  // Get all the files in the directory and subdirectory
  def getAllFiles(path: String): ArrayBuffer[String] = {
    val arr = ArrayBuffer[String]()
    val hdfs = fileSystem
    val getPath = getFilesAndDirs(path)
    getPath.foreach(patha => {
      if (hdfs.getFileStatus(patha).isFile)
        arr += patha.toString
      else {
        arr ++= getAllFiles(patha.toString)
      }
    })
    arr
  }
  def isFileExist(s: String): Boolean = {
    try {
      fileSystem.getFileStatus(new Path(s)).isFile || fileSystem.getFileStatus(new Path(s)).isDirectory
    } catch {
      case e: Exception => false
    }
  }
  def mergeSourceToDest(filesource: String, filedest: String): Unit = {
    println(f"---------- Merging from:  ${filesource}")
    val sourcepath = new Path(filesource)
    val in = fileSystem.open(sourcepath)
    val destpath = new Path(filedest)
    val isDestExist = isFileExist(filedest)
    val out:FSDataOutputStream = if (isDestExist) {
      println(f"---------- Merging to existing file:  ${filedest}")
      fileSystem.append(destpath)
    } else {
      println(f"---------- Merging to new file:  ${filedest}")
      fileSystem.create(destpath)
    }
    val b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }
  println("-"*100)
  println("Removing ods files:")
  removeFile("/ods")
  println("-"*100)
  println("Creating ods directory:")
  createFolder("/ods")
  println("-"*100)
  println("Initial files:")
  println(getAllFiles("/").mkString("\n"))
  println("-"*100)
  val pathSource: String = "/stage"
  println("List all dir in stage")
  for (dir <- getFilesAndDirs("/stage")) {
    val newDir: String = dir.toString.replace("/stage","/ods")
    val isNewDirExists = isFileExist(newDir)
    if (isNewDirExists) {
      println("-"*100)
      println(f"!!! WARNING !!! DIRECTORY ${newDir} ALREADY EXISTS!!!")
      println("-"*100)
    } else {
      println(f"------------------ Creating new dir: ${newDir}")
      createFolder(newDir)
      println("-"*100)
    }
    //Processing files in directory
    for(file <- getFiles(dir.toString)) {
      println(dir + ": " + file)
      if (file.takeRight(4) == ".csv") {  //processing only *.csv files
        println("-"*100)
        mergeSourceToDest(file, newDir + "/" + "part-0000.csv")
        println("-"*100)
        println(f"File: ${file} proceed to directory: ${newDir}")
        println("-"*100)
        removeFile(file)
      }
    }
  }
  println("-"*100)
  println("Files after batch processing:")
  println(getAllFiles("/").mkString("\n"))
  println("-"*100)

}
