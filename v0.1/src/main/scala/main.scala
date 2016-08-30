import SparqlSpark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.io.Source 
import bleibinhaus.hdfsscalaexample._
import java.util.Calendar

object Main {
	class MyHashMap[A, B](initSize : Int) extends mutable.HashMap[A, B] {
	  override def initialSize: Int = initSize // 16 - by default
	}
	
	def show(x: Option[Long]) = x match {
		case Some(s) => s
		case None => 0L
	}
	
	def main(args: Array[String]) {
		println("TIME START "+Calendar.getInstance().getTime())
		var conf = new SparkConf().setAppName("Spar(k)ql")
		val sc = new SparkContext(conf)

		var edgeArray = Array(Edge(0L,0L,"http://dummy/URI"))

		println("TIME READ VERTEX START "+Calendar.getInstance().getTime())
		
//----------------read version Converted Graph
/*		val vertexRDD: RDD[(VertexId,RDFVertex)] = 
			sc.textFile(args(0)).map(line => {
				var vertex = line.split("\\s+")
				var rdfv = new SparqlSpark.RDFVertex(vertex(0).toLong,vertex(1))
				vertex = vertex.slice(2,vertex.length)
				vertex.foreach(props => {
					var proObj = props.split("##PropObj##")
					rdfv.addProps(proObj(0),proObj(1))
				})
			(rdfv.id,rdfv)
		})
		
		
		println("TIME EDGE LOOP START "+Calendar.getInstance().getTime())
		val edgeLoopRDD: RDD[Edge[String]] = vertexRDD.map(vertex => {
			Edge(vertex._1,vertex._1,"LOOP")
		})
		println("TIME READ EDGE START "+Calendar.getInstance().getTime())
		var edgeRDD: RDD[Edge[String]] = 
			sc.textFile(args(1)).map(line => {
				val edge = line.split("\\s+")
				Edge(edge(0).toLong,edge(1).toLong,edge(2))
			})*/
//--------------read version Converted Graph

//--------------read version N3
//create vertex
		val tripleRDD = sc.textFile(args(0)).map(line => {
			val l = line.split("\\s+")
			(l(0), l(1), l(2))
		})
		
		//create vertexes
		val nodes = sc.union(tripleRDD.flatMap(x => List(x._1, x._3))).distinct		
		val vertexIds = nodes.zipWithUniqueId

		var NodesIdMap = vertexIds.collectAsMap()
		
		val vertexes = vertexIds.map(iri_id => {
			(iri_id._1, new SparqlSpark.RDFVertex(iri_id._2, iri_id._1))
		})
		//Subject vertexes with properties
		val vertexSRDD: RDD[(Long,RDFVertex)] = 
			tripleRDD.map(triple => (triple._1, (triple._2, triple._3)))
			.join(vertexes)
			.map(t => {				//(iri, ( (p, o), rdfvertex  ))
				val p_o = t._2._1
				val vertex = t._2._2
				if (p_o._2(0) == '"') {
					//ide kell hogy add property
					vertex.addProps(p_o._1,p_o._2);
				//type property
				} else if (p_o._1 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
					vertex.addProps(p_o._1,p_o._2);
				//object property
				} else {

				}
				(vertex.id, vertex)
			}).distinct
			
			//object vertexes
			val vertexORDD: RDD[(Long,RDFVertex)] = 
			tripleRDD.map(triple => (triple._3, ()))
			.join(vertexes)
			.map(t => {				//(iri, ( (), rdfvertex  ))
				val vertex = t._2._2
				(vertex.id, vertex)
			}).distinct
		
		val vertexRDD: RDD[(Long,RDFVertex)] = vertexORDD.union(vertexSRDD).reduceByKey((a,b) => {
		  if (a.props.length > b.props.length) {
		    a
		  } else {
		    b
		  }
		})

		vertexRDD.persist(StorageLevel.MEMORY_AND_DISK)

		val edgeLoopRDD: RDD[Edge[String]] = vertexRDD.map(vertex => {
			Edge(vertex._1,vertex._1,"LOOP")
		})
		
		var edgeRDD: RDD[Edge[String]] = 
			tripleRDD.map(s_p_o => {
				if (s_p_o._3(0) != '"' && s_p_o._2 != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
					Edge(show(NodesIdMap.get(s_p_o._1)), show(NodesIdMap.get(s_p_o._3)), s_p_o._2)
				} else {
					Edge(0,0,"")
				}
			}).distinct.filter(e => e.srcId !=0)
		 edgeRDD.persist(StorageLevel.MEMORY_AND_DISK)
		
		//NodesIdMap = NodesIdMap.empty
		vertexes.unpersist()
		vertexIds.unpersist()
		nodes.unpersist()

//--------------read version N3

		val query = args(2)

		println("QUERY: "+query)
		println("TIME CREATE PLAN START "+Calendar.getInstance().getTime())
		val planRes = SparqlPlan.createPlan(query)
		val plan = planRes.plan
		val rootNode = planRes.rootNode
		val varNum:Int = planRes.numVar
		if (plan.size>0) {
			val allP = plan(0).map(triple => {triple.tp.p})		
			edgeRDD = edgeRDD.filter( edge => allP.contains(edge.attr))
		} else {
			edgeRDD = sc.emptyRDD[Edge[String]];
		}
		println("TIME EDGE UNION START "+Calendar.getInstance().getTime())
		edgeRDD = edgeRDD.union(edgeLoopRDD)
	
		println("TIME CREATE GRAPH START "+Calendar.getInstance().getTime())
		
		val graph = Graph(vertexRDD,edgeRDD)
		println("TIME CREATE GRAPHOPS START "+Calendar.getInstance().getTime())

		val head = mutable.LinkedHashMap[String,Int]()
		var rows = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
		val initMsg = mutable.LinkedHashMap.empty[String,RDFTable]
		val emptyTable = new RDFTable(head,rows)

		def msgCombiner(a: mutable.LinkedHashMap[String,RDFTable], b: mutable.LinkedHashMap[String,RDFTable]):mutable.LinkedHashMap[String,RDFTable] = {
			b.foreach(i => {
				if (a.contains(i._1)){
					a(i._1) = a.getOrElseUpdate(i._1,emptyTable).merge(i._2) //.clone()
				} else {
					a(i._1) = i._2 //.clone()
				}
			})
			return a
		}

		//EdgeTriplet[x,y]   -- x nodetype, y edgetype
		def sendMsg(edge: EdgeTriplet[RDFVertex,String]): Iterator[(VertexId, mutable.LinkedHashMap[String,RDFTable])] = {
			var iteration = edge.dstAttr.getIter()
			if (edge.srcAttr.iter>edge.dstAttr.iter) { 
				iteration = edge.srcAttr.getIter()
			}
			var i:Iterator[(VertexId, mutable.LinkedHashMap[String,RDFTable])] = Iterator.empty
			var i_withoutAlive:Iterator[(VertexId, mutable.LinkedHashMap[String,RDFTable])] = Iterator.empty
			var tmp = "Iteration "+Calendar.getInstance().getTime();
			
			if (iteration < plan.length) {
				plan(iteration).foreach(triple => {
					var triplePattern = triple.tp
					var tablePattern = triple.headPattern
					if (edge.attr == "LOOP" && edge.srcAttr.props.exists(vp => {vp.prop == triplePattern.p})) {
						if (triple.src == " ") {
							i = i ++ Iterator((edge.dstAttr.id,initMsg)) 
						} else {
							var m = edge.dstAttr.mergeEdgeToTable(
									triplePattern.s,triplePattern.o,
									triplePattern.s,triplePattern.o,edge.srcAttr.uri,edge.srcAttr.props.find(vp => {vp.prop == triplePattern.p}).get.obj,iteration)
							i = i ++ Iterator((edge.srcAttr.id,m))
							i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id,m))
						}
					} else if (edge.attr == triplePattern.p) {
						//ALIVE message
						if (triple.src == " ") {
							i = i ++ Iterator((edge.dstAttr.id,initMsg))
						//SEND forward
						} else if (triple.src == triplePattern.s) {
							if (tablePattern.forall(a => edge.srcAttr.tableMap.contains(triplePattern.s) && edge.srcAttr.tableMap(triplePattern.s).head.contains(a)) && 
							edge.srcAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.s,new mutable.MutableList[VertexProp]())) &&
							edge.srcAttr.checkObjectProperty(triplePattern.s) && edge.dstAttr.checkObjectProperty(triplePattern.o)) {
								var m = edge.srcAttr.mergeEdgeToTable(
										triplePattern.o,triplePattern.s,
										triplePattern.s,triplePattern.o,edge.srcAttr.uri,edge.dstAttr.uri,iteration)
								i = i ++ Iterator((edge.dstAttr.id,m))
								i_withoutAlive = i_withoutAlive ++ Iterator((edge.dstAttr.id,m))
							}
						//SEND backward
						} else {
							if (tablePattern.forall(a => edge.dstAttr.tableMap.contains(triplePattern.o) && edge.dstAttr.tableMap(triplePattern.o).head.contains(a)) && 
							edge.dstAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.o,new mutable.MutableList[VertexProp]())) &&
							edge.dstAttr.checkObjectProperty(triplePattern.o) && edge.srcAttr.checkObjectProperty(triplePattern.s)) {
								var m = edge.dstAttr.mergeEdgeToTable(
										triplePattern.s,triplePattern.o,
										triplePattern.s,triplePattern.o,edge.srcAttr.uri,edge.dstAttr.uri,iteration)
									i = i ++ Iterator((edge.srcAttr.id,m))
									i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id,m))
							}
							i = i ++ Iterator((edge.dstAttr.id,initMsg))
						}
					} else {
						//Iterator.empty
					}
				})
			}
			return i
		}

        def vertexProgram(id: VertexId, attr: RDFVertex, msgSum: mutable.LinkedHashMap[String,RDFTable]):RDFVertex = {
			attr.mergeMsgToTable(msgSum)
			attr.iter = attr.iter + 1
			return attr.clone()
		}

		var startTime = System.currentTimeMillis()
		println("TIME PREGEL START "+Calendar.getInstance().getTime())
		var result = Pregel(graph,initMsg,Int.MaxValue,EdgeDirection.Either)(vertexProgram,sendMsg,msgCombiner)
		var withResult = true;
		if (!withResult) {
			println("WITHOUT RESULT "+Calendar.getInstance().getTime());
			System.exit(1);
		}
		println("RES "+Calendar.getInstance().getTime());
		val rootNodeDataProp = planRes.dataProperties.getOrElse(rootNode,new mutable.MutableList[VertexProp]())

		if (plan.size == 0) {
			var res = result.vertices.filter(v => v._2.checkDataProperty(rootNodeDataProp))
			println("RESULT1: "+res.count());
		} else {
			println("before REsult:"+result.vertices.count())
			println("plan size: "+plan.size)
			println("root node: "+rootNode)
			var res2 = result.vertices.filter(v => (v._2.tableMap.contains(rootNode) 
				&& (v._2.tableMap(rootNode).iteration.size == plan.size)
				&& (v._2.tableMap(rootNode).rows.size > 0)
					))
			println("before2 REsult:"+res2.count())
			
			res2 = res2.filter(v => (
					v._2.checkDataProperty(rootNodeDataProp)
					))
			if (res2.count() > 0) {
				var rowNum = 0;
				res2.collect().foreach(v => {
					rowNum = rowNum + v._2.tableMap(rootNode).rows.size
				})
				println("RESULT2: "+rowNum)
/*				var helpHead = res2.first()._2.tableMap(rootNode).head.clone()

				helpHead.foreach(h => print(h._1+" "))
				println("")
				res2.foreach(v => {
					v._2.tableMap(rootNode).rows.map(row => {
						var t = "###"
							helpHead.foreach(varName => {
								t+= row(v._2.tableMap(rootNode).head(varName._1))+" "
							})
						println(t)
					})
				})

				println("-------------")*/
			} else {
				println("RESULT3: 0")
			}
		}
		var stopTime = System.currentTimeMillis();
		println("TIME STOP "+Calendar.getInstance().getTime())
		println("Elapsed Time: "+(stopTime-startTime))

	}
}
