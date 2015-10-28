package com.parser

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.io.Source
import java.io.File
import scala.util.matching.Regex

/**
 * @author sabo
 */
object Test {
  
  import com.parser.Dependency._
  
  var conn: Connection = _
  var statement: Statement = _
  
  def init(): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn_str = "jdbc:mysql://localhost:3306/test?user=root&password=pwd"
    conn = DriverManager.getConnection(conn_str)
    statement = conn.createStatement
    println("connection done.")
  }
  
  def destroy {
    conn.close
    statement.close
    println("close connection.")
  }
  
  def main(args: Array[String]): Unit = {
    val file = new File("/code/path")
    
    val allFile = recursiveListFiles(file, """.*hive.*\.sql$""".r)
    init
    allFile.foreach(f =>
      //println(f.getPath)
        try {
          val querys = Source.fromFile(f).mkString
          var hqls = replaceVariableDep(querys)
          
          hqls.foreach { hql => 
            val dep = process(hql)
            //println(hql)
            insert(hql, dep.sources.mkString(","), dep.destinations.mkString(","), f.getPath)
          }
          
        } catch {
          case t: Throwable => println("can't read file.") // TODO: handle error
        }
    )
    destroy
    
  }
  
  def insert(query: String, sources: String, destinations: String, path: String) {
    val prep = conn.prepareStatement("INSERT INTO dependency (query, sources, destinations, path) VALUES (?, ?, ?, ?) ")
    prep.setString(1, query)
    prep.setString(2, sources)
    prep.setString(3, destinations)
    prep.setString(4, path)
    prep.executeUpdate
  }
  
  def recursiveListFiles(f: File, r: Regex): Array[File] = {  
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }  
  
}