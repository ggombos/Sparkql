import scala.collection.mutable._
import SparqlSpark._
import scala.collection.concurrent._

object SparqlPlan {
    class Triple(val tp:String) extends java.io.Serializable {
        var spo = tp.split(" ")
        val s:String = spo(0)
        val p:String = spo(1)
        val o:String = spo(2)
        val str:String = tp
        var finish:Boolean = false
		
		override def toString():String ={
            return str
        }
    }
	
	class PlanResult(_plan:ArrayBuffer[ListBuffer[PlanItem]], _rootNode:String, _varNum:Int, _dataProperties:LinkedHashMap[String,MutableList[VertexProp]]) extends java.io.Serializable {
		val plan:ArrayBuffer[ListBuffer[PlanItem]] = _plan.clone()
		val rootNode:String = _rootNode
		val numVar:Int = _varNum
		val dataProperties = _dataProperties.clone()
	}
	
	class PlanItem(_tp:Triple,_src:String,_headPattern:Set[String]) extends java.io.Serializable {
		val tp:Triple = _tp
		val src:String = _src
		var headPattern:Set[String] = _headPattern
	}

	def getDataProperties(SparqlQuery:String):(Array[Triple],LinkedHashMap[String,MutableList[VertexProp]])= {
	    var query = SparqlQuery.split(" . ")
        var TPs:Array[Triple] = query.map(tp => new Triple(tp))
	    var queryArray:Array[String] = Array[String]()
	    var dataProperties:LinkedHashMap[String,MutableList[VertexProp]] = LinkedHashMap[String,MutableList[VertexProp]]()
	    TPs = TPs.flatMap(tp => {
	        if ((tp.p == "rdf:type") || (tp.p == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")||
	            tp.o(0) == '"' ) {
	            var propList = dataProperties.getOrElseUpdate(tp.s, MutableList[VertexProp]())
				propList += new VertexProp(tp.p,tp.o)
	            dataProperties(tp.s) = propList
	            Array[Triple]()
	        } else {
                Array[Triple](tp)
	        }
	    })

	    return (TPs,dataProperties)
	}
	
	def createPlan(SparqlQuery: String):PlanResult = {
        var (tps,dataProperties):(Array[Triple],LinkedHashMap[String,MutableList[VertexProp]]) = getDataProperties(SparqlQuery)
//       var query:Array[String] = SparqlQuery.split(" . ")
       var plan = ArrayBuffer[ListBuffer[PlanItem]]()
       var aliveTP = ArrayBuffer[ListBuffer[PlanItem]]()
	   //osszes valtozo ebben van, ebben a plan generatorba, mert a q csak 1 elemet tartalmazhat
	   var q2 = new Queue[String]()
       var iteration = ListBuffer[PlanItem]()
       var aliveIter = ListBuffer[PlanItem]()
       var level:Int = 0
       var rootNode = ""
	   //az aktualis valtozo
	   var v = ""
	   var vars:Set[String] = HashSet()
	   if (tps.size>0) {
			v = tps(0).s
			rootNode = tps(0).s
			vars = HashSet(tps(0).s,tps(0).o)
	   } else {
			rootNode = dataProperties.keySet.head
	   }
	
	   var MsgPattern:LinkedHashMap[String,Set[String]] = LinkedHashMap[String,Set[String]]()
       while (v != "") {
           //lementjuk a valtozokat
           if (!MsgPattern.contains(v)) {
                MsgPattern(v) = HashSet()
           }
		   //talaltunk megfelelo tp-t
           var found:Boolean = false
		   //kell meg ez a valtozo, mert van meg tp amiben szerepel
		   var need:Boolean = false
           tps.map(tp => {
               vars += tp.s
               vars += tp.o
               if (!tp.finish && tp.s == v) {
                   if (found) {
						need = true
                   } else {
						if (!q2.contains(tp.o)) q2.enqueue(tp.o)
                       aliveIter += new PlanItem(new Triple("?alive "+tp.p+" ?alive")," ",Set[String]())
                       iteration += new PlanItem(new Triple(tp.str),tp.o,Set[String]())
                       tp.finish = true
                   }
                   found = true
               } else if (!tp.finish && tp.o == v) {
                   if (found) {
						need = true
                   } else {
						if (!q2.contains(tp.s)) q2.enqueue(tp.s)
                        aliveIter += new PlanItem(new Triple("?alive "+tp.p+" ?alive")," ",Set[String]()) 
                        iteration += new PlanItem(new Triple(tp.str),tp.s,Set[String]())
                        tp.finish = true
                   }
                   found = true
               } else {
                   //nop
               }

           })
			if (!need) {
				if (!q2.isEmpty)	{
					v = q2.dequeue
				} else {
					v = ""
				}
			}
			if (!iteration.isEmpty) {
				aliveTP += aliveIter
				if (aliveTP.length>1) {
					for (i<- 0 to level-1) {
						aliveTP(i).map(alive => iteration += alive)
					}
				}
				plan += iteration
			}
			aliveIter = ListBuffer[PlanItem]()
			iteration = ListBuffer[PlanItem]()
			level = level+1
       }
       plan = plan.reverse
       
       plan = plan.map(iter => {
           iter.map(planitem => {
               if (planitem.tp.s != "?alive") {
                   var o = planitem.tp.o
                   var src = planitem.src
                   var des = o
                   if (src == o) {
                        des = planitem.tp.s
                   }
                    MsgPattern(des) = MsgPattern(des).union(MsgPattern(src))
                    MsgPattern(des) += src
                    var mp:Set[String] = Set[String]()
                    MsgPattern(src).foreach(i => mp += i)
                    planitem.headPattern = mp
					planitem
               } else {
                    planitem.headPattern = Set[String]()
					planitem
               }
           })
       })
  
       plan.map(list => { 
            println("PLAN-- [")
            list.map(tp => println("PLAN-- ("+tp.tp.toString()+", "+tp.src+", "+tp.headPattern+")"))
            println("PLAN-- ],")
       }
       )
	   println("PLAN-- DATAPROPERTIES")
	   dataProperties.map(v => {
				println("PLAN-- "+v._1)
				v._2.map(p => {
					println("PLAN-- "+p.prop+" "+p.obj)
				})
			}
	   )

		return new PlanResult(plan,rootNode,vars.size,dataProperties)
	}
}


