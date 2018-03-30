import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}

import scala.Array._
import scala.reflect.ClassTag

object newdraft {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val filename = "tempFiles.txt"
//  val writer = new FileWriter(new File("Chang_Fan_communities.txt"))
//  val writer2 = new FileWriter(new File("Chang_Fan_betweenness.txt"))
  val conf = new SparkConf().setAppName("Chang_Fan_HW5").setMaster("local")
  val sc = new SparkContext(conf)
  var c = 0
  var betweennessMatrix = Array.ofDim[Double](7 + 1, 7 + 1)


  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()
    val lines = sc.textFile(args(0))
    val header = lines.first()
    val input = lines.filter(line => line != header)
    val splitInput = input.map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt))
    val userData = splitInput.groupByKey().sortByKey().map(x => (x._1, x._2.toSet))
    val pairs = userData.cartesian(userData).filter(x => x._1._1 != x._2._1)
    val intersection = pairs.map(x => (x._1._1, x._2._1, x._1._2.intersect(x._2._2)))
    val friends = intersection.filter(x => x._3.size >= 3).map(x => (x._1, x._2)) // (user1, user2)
    val vertices = userData.map(x => (x._1.toLong, 0.0)) // 注意第一列为vertexId，必须为Long，第二列为顶点属性，可以为任意类型，包括Map等序列。
    val edges = friends.map(x => Edge(x._1, x._2, 1.0)) // 起始点ID必须为Long，最后一个是属性，可以为任意类型
    //    val graph: Graph[Double, Double] = Graph(vertices, edges) // （（点的属性），边的属性）
    //    graph.edges.foreach(x => writer0.write(x.toString.substring(5, x.toString.length - 5).replaceAll(",", " ") + "\n"))
    //    writer0.close()
    val graph2 = GraphLoader.edgeListFile(sc, filename)
    println("Building Graph...")
    //val graph2 = Graph.fromEdges(edges, 0.0)
    val numOfUser = graph2.vertices.count().toInt
    //var record = graph2.edges.map(x => ((x.srcId, x.dstId), 0.0))
    //var edgesWithCredit = GraphLoader.edgeListFile(sc, filename).mapEdges(x => 0.0).edges

    //path.delete()
    //println(numOfUser + " " + betweennessMatrix(0).length)

    for (i <- 1 to numOfUser) { //1-671
      val rootID: VertexId = i
      val initialGraph2 = graph2.mapVertices((id, _) => if (id == rootID) 0.0 else Double.PositiveInfinity).mapEdges(x => x.attr.toDouble)
      println("Current Root is " + i)
      val t1 = System.currentTimeMillis()
      val shorestPath = initialGraph2.pregel(initialMsg = Double.PositiveInfinity)(
        (id, dist, newDist) => math.min(dist, newDist),
        triplet => {
          if (triplet.srcAttr + 1.0 < triplet.dstAttr) { // srcAttr：邻居到根的距离；attr：自己到邻居的边长；desAttr：自身到根的距离
            Iterator((triplet.dstId, triplet.srcAttr + 1.0)) //
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b))
      val t2 = System.currentTimeMillis()
      //printf("shortest path 2 : %d ms.\n", t2 - t1)

      val DAGgraph = shorestPath.subgraph(vpred => vpred.srcAttr != vpred.dstAttr) // 一条边的两顶点bfs结果不同才保留
      val depth = shorestPath.vertices
      //      val SpPlusDepth = DAGgraph.pregel(0.0, 1, EdgeDirection.Out)( // 最短路径个数+层数
      //        vprog = (id, attr, msg) => attr + msg,
      //        sendMsg = triplet => {
      //          if (triplet.srcAttr == triplet.dstAttr - 1) { // 如果目标在自己下一层
      //            Iterator((triplet.dstId, 1))
      //          }
      //          else {
      //            Iterator.empty
      //          }
      //        },
      //        mergeMsg = (a, b) => a + b)
      //      val SpPlusDepth1 = SpPlusDepth.mapVertices((id, num) => if (id == rootID) 1 else num)
      //      val numOfSp = SpPlusDepth1.joinVertices(depth) {
      //        case (id, x, y) => x - y // 减掉层数
      //      }
      val t3 = System.currentTimeMillis()
      //printf("num of Sp : %d ms.\n", t3 - t2)

      val numOfSp = DAGgraph.pregel(Double.PositiveInfinity, 1, EdgeDirection.Out)(
        vprog = (id, attr, msg) => {
          //println("count is " + c.toString + "check" +  id + " " + msg.toString)
          if (c == 0) {
            math.min(attr, msg)
          }
          else {
            msg
          }
        },
        sendMsg = triplet => {
          if (triplet.srcAttr == triplet.dstAttr - 1) { // 如果目标在自己下一层
            Iterator((triplet.dstId, 1))
          }
          else {
            Iterator.empty
          }
        },
        mergeMsg = (a,b) => {
          //println("merge! is "+c)
          c = c + 1
          a + b
        } )
      c = 0
      val SpPlusDepth2 = numOfSp.mapVertices((id, num) => if (id == rootID) 1 else num)
      //SpPlusDepth2.vertices.foreach(println)

      val DAGdegree = SpPlusDepth2.degrees.map(x => (x._1, x._2 / 2)) // 所有节点的度信息

      val leafNode = numOfSp.joinVertices(DAGdegree) {
        case (id, num, degree) => degree - num
      }.vertices.filter(x => x._2 == 0.0) // 所有叶节点

      val initialCredit = numOfSp.joinVertices(leafNode) { //将叶节点置0，其他节点最短路径数至少为1
        case (id, num, zero) => num * zero
      }.mapVertices((id, value) => if (value == 0) 1.0 else 0.0) //属性为0的点是叶节点，设为1，其他为0

      val NumAndDepth = numOfSp.outerJoinVertices(depth) { //（id，路径数，深度）
        case (id, num, Some(dep)) => (num, dep)
        case (id, num, None) => (num, -1.0)
      }
      val InitCreNumDep = initialCredit.outerJoinVertices(NumAndDepth.vertices) { // (id,(cre,num,dep))
        case (id, cre, Some(x)) => (cre, x._1, x._2)
        case (id, cre, None) => (cre, -1.0, -1.0)
      }
      val t4 = System.currentTimeMillis()
      //printf("join : %d ms.\n", t4 - t3)

      val CreNumDep = InitCreNumDep.pregel((-1.0, -1.0, -1.0), 3, EdgeDirection.In)( // (id,(cre,num,dep))
        vprog = (id, attr, msg) => {
          //println("active ID: " + id + " count: " + c + " msg is " + msg)
          if (c == 0) {
            (attr._1, attr._2, attr._3)
          }
          else {
            (1 + msg._1, attr._2, attr._3)
          }
        },
        sendMsg = triplet => {
          //println("check " + triplet.srcAttr._3 + " " + triplet.dstAttr._3)
          if (triplet.srcAttr._3 == triplet.dstAttr._3 + 1.0) { // 自己不为0分，目标在自己上一层
            Iterator((triplet.dstId, (triplet.srcAttr._1 * triplet.dstAttr._2 / triplet.srcAttr._2, 0, 0))) // 发送自己的分数，其他两属性没用
          } else {
            Iterator.empty
          }
        },
        mergeMsg = (a, b) => {
          c = c + 1
          (a._1 + b._1, 0, 0)
        })
      c = 0
      //CreNumDep.vertices.foreach(println)
      //CreNumDep.edges.foreach(println)
      val t5 = System.currentTimeMillis()

      //printf("Betweenness : %d ms.\n", t5 - t4)
      //      val finalGraph = CreNumDep.mapTriplets(triplet => { // 给边赋值
      //        if (triplet.srcAttr._3 > triplet.dstAttr._3) { // 向上指的边
      //          triplet.srcAttr._1 * triplet.dstAttr._2 / triplet.srcAttr._2
      //        } else {    //向下的边
      //          triplet.dstAttr._1 * triplet.srcAttr._2 / triplet.dstAttr._2
      //        }
      //      })
      //      //finalGraph.vertices.foreach(println)
      //      record = record.union(finalGraph.edges.map(x => ((x.srcId, x.dstId), x.attr))).reduceByKey(_+_)
      //      val tt = System.currentTimeMillis()
      //      printf("assign values to edge : %d ms.\n", tt - t5)

      val verticesInfo = CreNumDep.vertices.collect() // 顶点值分配完毕，准备计算边的值,(id,(cre,num,dep))
      def getBetween(input1: Int, input2: Int): Double = {
        val src = verticesInfo.filter(line => line._1.toInt == input1).map(line => line._2) // (cre,num,dep)
        val dst = verticesInfo.filter(line => line._1.toInt == input2).map(line => line._2)
        var result = 0.0
        if (src(0)._3 > dst(0)._3) { // i在j下层
          if (src(0)._2 > 1) {
            result = dst(0)._2.toDouble / src(0)._2.toDouble * src(0)._1.toDouble
          } else {
            result = src(0)._1
          }
        }
        else if (src(0)._3 < dst(0)._3) {
          if (dst(0)._2 > 1) {
            result = src(0)._2.toDouble / dst(0)._2.toDouble * dst(0)._1.toDouble
          } else {
            result = dst(0)._1
          }
        }
        betweennessMatrix(input1)(input2) = betweennessMatrix(input1)(input2) + result
        betweennessMatrix(input2)(input1) = betweennessMatrix(input2)(input1) + result
        result + betweennessMatrix(input1)(input2)
      }

      val temp = CreNumDep.mapEdges(x => getBetween(x.dstId.toInt, x.srcId.toInt)).edges.collect()
      val t6 = System.currentTimeMillis()
      //printf("build matrix : %d ms.\n", t6 - t5)
    } // if end
    //val tx = System.currentTimeMillis()
    //println("loop end time : " + (tx - t0))

    println("computing modularity...")
    var edgeWithBet = Array[Edge[Double]]() // 存放所有的相连的边
    var threshold = 0.0
    for (i <- 1 to numOfUser) {
      for (j <- i + 1 to num  OfUser) {
        if (betweennessMatrix(i)(j) != 0) {
          betweennessMatrix(i)(j) = betweennessMatrix(i)(j) / 4
          betweennessMatrix(j)(i) = betweennessMatrix(i)(j)
          println("i,j is " + i + "," + j + " val is " + betweennessMatrix(i)(j))
          edgeWithBet = edgeWithBet :+ Edge(i.toLong, j.toLong, betweennessMatrix(i)(j))
          edgeWithBet = edgeWithBet :+ Edge(j.toLong, i.toLong, betweennessMatrix(i)(j))
          if (betweennessMatrix(i)(j) > threshold) threshold = betweennessMatrix(i)(j)
//          writer2.write("(" + i + "," + j + "," + betweennessMatrix(i)(j) + ")\n")
        }
      }
    }
//    writer2.close()
    //println(threshold)
    val edgeWithBetweenness = sc.parallelize(edgeWithBet)
    def getModularity[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): (Int, Double) = {
      graph.cache()
      val factor = 0.5 / graph.numEdges.toDouble
      val connectedComponent: Iterable[Iterable[VertexId]] =
        graph
          .subgraph(epred = (et) => (et.srcId < et.dstId))
          .connectedComponents()
          .vertices
          .map(pair => (pair._2, pair._1))
          .groupByKey
          .collect
          .map(pair => pair._2)

      val verticesWithDegAndNeigh: VertexRDD[(Int, Array[VertexId])] =
        graph.inDegrees.innerJoin(graph.collectNeighborIds(EdgeDirection.Out))((id, inDeg, neighbors) => (inDeg, neighbors))

      val filteredRDDs: Iterable[VertexRDD[(Int, Array[VertexId])]] =
        connectedComponent
          .map(connectedVertexList => {
            verticesWithDegAndNeigh
              .filter(pair => connectedVertexList.toArray.contains(pair._1))
          })

      val modularityOfComponent: Iterable[Double] =
        filteredRDDs
          .map(vertexRDD => {

            val vertexPairs = vertexRDD.cartesian(vertexRDD)
            vertexPairs
              .map(pair => {
                val nodeI: (VertexId, (Int, Array[VertexId])) = pair._1
                val nodeJ: (VertexId, (Int, Array[VertexId])) = pair._2
                val nullValue: Double = (nodeI._2._1 * nodeJ._2._1).toDouble * factor
                if (nodeI._2._2.contains(nodeJ._1)) 1.0 - nullValue
                else -nullValue
              })
              .fold(0.0)(_ + _)
          })
      (modularityOfComponent.toSeq.length, factor * modularityOfComponent.fold(0.0)(_ + _))
    }
    var removeHighBet = Graph.fromEdges(edgeWithBetweenness, 0.0).subgraph(epred => epred.attr < threshold * 0.12 - 0.01)
    //    var oldModularity = getModularity(removeHighBet)
    //println("initial modularity " + oldModularity)
    //    var flag = true
    //    var count = 0
    //    while (flag) {
    //      threshold = threshold * 0.8
    //      println("current threshold is " + threshold)
    //      removeHighBet = Graph.fromEdges(edgeWithBetweenness, 0.0).subgraph(epred => epred.attr < threshold)
    //      val newModularity = getModularity(removeHighBet)._2
    //      println(newModularity)
    //      //if (newModularity - oldModularity > 0.001) flag = false
    //    }
    val cc = removeHighBet.connectedComponents() // cc是分开的连通子graph
    val ccvertices = cc.vertices.map(x => (x._2, x._1)) // ccvertices是RDD (社区号，ID)
    val community = ccvertices.groupByKey().sortByKey().collect().map(x => {
      val buffer = x._2
      val member = buffer.map(x => x)
      member
    })
//    community.foreach(x => writer.write(x.toList.sorted.toString().substring(4, x.toString().length) + "\n"))
//    writer.close()
  }
}

