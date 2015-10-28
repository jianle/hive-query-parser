package com.parser

import scala.collection.mutable.HashSet
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.ql.parse.ASTNode
import org.apache.hadoop.hive.ql.parse.HiveParser
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.json4s.JsonDSL.pair2Assoc
import org.json4s.JsonDSL.seq2jvalue
import org.json4s.jvalue2monadic
import scala.collection.mutable.ListBuffer

/**
 * @author sabo
 */

case class TableInfo(db: String, table: String, token: String)

object TreeParser {
  val PTRN_TOKEN = "TOK_(.*)".r
  var currentDB = "default"
  
  def parse(tree: ASTNode): Dependency = {
    var result = new Dependency()
    
    tree.getToken().getType() match {
      case HiveParser.TOK_SWITCHDATABASE => this.currentDB = tree.getChild(0).getText()
      case HiveParser.TOK_QUERY => parseRoot(tree, result)
      case HiveParser.TOK_UNION => 
      case HiveParser.TOK_LOAD => {
        val tbl = tree.getChild(1).asInstanceOf[ASTNode]
        result.addDestinations(parseTableRef(tbl))
      }
      
      case HiveParser.TOK_DROPTABLE => result.addDestinations(parseTableRef(tree))
      
      case HiveParser.TOK_CREATETABLE => parseCreate(tree, result)
      
      case _ => parseOther(tree, result)
    }
    result
  }
  
  def parseCreate(tree: ASTNode, result: Dependency ) = {
    var a = 0
    for(a <- 0 to tree.getChildren().size() - 1) {
      var child = tree.getChildren().get(a).asInstanceOf[ASTNode]
      child.getToken().getType() match {
        case HiveParser.TOK_TABNAME => {
          result.addDestinations(parseTableRef(tree))
        }
        case HiveParser.TOK_QUERY => parseQuery(child, result)
        case _ => 
      }
    }
    result.addDestinations(parseTableRef(tree))
  }
  
  def parseOther(tree: ASTNode, result: Dependency ) = {
    val op = tree.getToken().getText.split("_")
    
    if (op contains "ALTERTABLE") {
      result.addDestinations(parseAlter(tree))
      
    } else if (op contains "SHOWPARTITIONS") {
      result.addSources(parseTableRef(tree))
      
    } else if (op contains "DESCTABLE") {
      val child = tree.getChild(0).getChild(0).asInstanceOf[ASTNode]
      result.addSources(parseTable(child, "DESCTABLE"))
      
    } else {
      println("Not Found." + op.toList)
    }
    
  }
  
  def parseAlter(tree: ASTNode) = {
    val db = currentDB
    val table = tree.getChild(0).getText
    val token = validateToken(tree.getText)
    TableInfo(db, table, token)
  }
  
  def parseRoot(tree: ASTNode, result: Dependency ) = {
    
    tree.getToken().getType() match {
      case HiveParser.TOK_UNION => parseUnion(tree, result)
      case HiveParser.TOK_QUERY => parseQuery(tree, result)
    }
    
  }
  
  def parseUnion(tree: ASTNode, result: Dependency ): Unit = {
    var a = 0;
    var node = tree.getChildren
    
    for(a <- 0 to node.size - 1) {
      var child = node.get(a).asInstanceOf[ASTNode];
      
      child.getToken().getType() match {
        case HiveParser.TOK_QUERY => parseQuery(child, result)
        case HiveParser.TOK_UNION => parseUnion(child, result)
      }
    }
  }
  
  def parseQuery(tree: ASTNode, result: Dependency ):Unit = {
    var a: Integer = 0;
    var from = tree.getChild(0).asInstanceOf[ASTNode];
    parseFrom(from, result);
    var node = tree.getChildren
    
    for(a <- 0 to node.size - 1) {
      parseInsert(tree.getChild(a).asInstanceOf[ASTNode], result)
    }

  }
  
  def parseFrom(tree: ASTNode, result: Dependency ) = {
    val child = tree.getChild(0).asInstanceOf[ASTNode]
    child.getToken().getType() match {
      case HiveParser.TOK_LEFTSEMIJOIN => parseJoin(child, result)
      case HiveParser.TOK_UNIQUEJOIN => 
      case _ => parseJoinOperand(child, result)
    }
  }
  
  
  def parseInsert(tree: ASTNode, result: Dependency ) = {
    val child = tree.getChild(0).getChild(0).asInstanceOf[ASTNode]
    
    if (child.getToken().getType() == HiveParser.TOK_TAB) {
      result.addDestinations(parseTableRef(child))
    }
    
  }
  
  
  
  def parseJoin(tree: ASTNode, result: Dependency ):Unit = {
    var a = 0;
    
    val node = tree.getChildren()
    
    for(a <- 0 to node.size-1) {
      var child = node.get(a).asInstanceOf[ASTNode];
      
      child.getToken().getType() match {
        case HiveParser.TOK_LEFTSEMIJOIN => parseJoin(child, result)
        case _ => parseJoinOperand(child, result)
      }
    }
  }
  
  def parseJoinOperand(tree: ASTNode, result: Dependency ):Unit = {
    
    tree.getToken().getType() match {
      case HiveParser.TOK_JOIN => parseJoin(tree, result)
      case HiveParser.TOK_LEFTOUTERJOIN => parseJoin(tree, result)
      case HiveParser.TOK_FULLOUTERJOIN => parseJoin(tree, result)
      case HiveParser.TOK_RIGHTOUTERJOIN => parseJoin(tree, result)
      case HiveParser.TOK_TABREF => result.addSources(parseTableRef(tree))
      case HiveParser.TOK_SUBQUERY => {
        val query = tree.getChild(0).asInstanceOf[ASTNode]
        parseRoot(query, result)
      }
      case _ => println("Filter this token:" + tree.getToken().getText)
      
    }
  }
  
  def parseTableRef(tree: ASTNode) = {
    val tbl = tree.getChild(0).asInstanceOf[ASTNode];
    val token = validateToken(tree.getText)
    parseTable(tbl, token)
  }
  
  def parseTable(tbl: ASTNode, token: String):TableInfo = {
    
    if (tbl.getChildren().size() == 2) {
      var db = tbl.getChild(0).getText
      var table = tbl.getChild(1).getText
      TableInfo(db, table, token)
    } else {
      var db = currentDB
      var table = tbl.getChild(0).getText
      TableInfo(db, table, token)
    }
  }
  
  def validateToken(token: String): String = token match {
    case PTRN_TOKEN(token) => {
      token
    }
    case _ => "None"
  }
  
}



class Dependency {
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  
  var sources: ListBuffer[TableInfo] = ListBuffer()
  var destinations: ListBuffer[TableInfo] = ListBuffer()
  
  def addSources(info: TableInfo) = {
    this.sources = this.sources:+(info)
  }
  
  def addDestinations(info: TableInfo) = {
    this.destinations = this.destinations:+(info)
  }
  
  def merge(dep: Dependency) = {
    
    this.sources ++= dep.sources
    this.destinations ++= dep.destinations
    this
  }
  
  def allInfo = {
    (this.destinations ++ this.sources).distinct
  }
  
  def getSources = this.sources.toList.distinct
  def getDestinations = this.destinations.toList.distinct
  
  def getJson() = {
    val json = 
      ("sources" -> 
        getSources.map { info => 
          (("db" -> info.db) ~
           ("table" -> info.table) ~
           ("token" -> info.token))}
      ) ~
      ("destinations" -> 
        getDestinations.map { info => 
          (("db" -> info.db) ~
           ("table" -> info.table) ~
           ("token" -> info.token))}
      )
    //json
    render(json)
  }
}


object Dependency {
  import TreeParser._
  
  lazy val hiveParseDriver = new ParseDriver
  
  def parseDep(hqls: List[String]) = {
    var n, ret = new Dependency()
    currentDB = "default"
    hqls.foreach { x => 
      n = process(x).asInstanceOf[Dependency]
      ret.merge(n)
    }
    ret
  }
  
  def process(hql: String): Dependency = {
    try {
      var tree = hiveParseDriver.parse(hql)
      tree = ParseUtils.findRootNonNullToken(tree)
      TreeParser.parse(tree)
    } catch {
      case t: Throwable =>  // TODO: handle error
        t.printStackTrace
      println("Hive SQL Parse Error")
      new Dependency
    }
  }
  
  /*
   * 替换SQL中出现的变量
   */
  def replaceVariableDep(sql: String) = {
    
    val ptrnBuffer = "(?i)^(SET|ADD\\s+JAR|CREATE\\s+TEMPORARY\\s+FUNCTION)\\s+".r
    
    val hqls = sql.replaceAll("(?s)/\\*.*?\\*/", "")   //替换注释
                  .replaceAll("\\$\\{.*?\\}", "''")    //替换变量
                  .split(";")                          //切分SQL
                  .map(_.trim).filter(_.nonEmpty)      //过滤
                  .filter(ptrnBuffer.findFirstIn(_).isEmpty)
                  .toList
    
    hqls
    
  }
  
  def main(args: Array[String]): Unit = {
    
    val sql = """
          desc test.dual;

          show databases;
          SET hive.auto.convert.join = true;
          set hive.exec.parallel=true;
          DROP TABLE IF EXISTS test.dual;
          SELECT  dummy
          FROM    tmp.dual
          ;
      """
    val hqls = replaceVariableDep(sql)
    
    val dps = parseDep(hqls)
    
    println(dps.sources)
    println(dps.destinations)
    println(dps.getJson \ "sources")
    println(dps.getJson \ "destinations")
    
  }
  
}

