package org.apache.spark.sql

import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, InputAdapter, SparkPlan}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ListBuffer

class SparkDot(plan: SparkPlan) {

  var nextCluster = 0

  def generate(filename: String): Unit = {

    // build list of query stages
    val stages = new ListBuffer[QueryStageExec]()
    def findQueryStages(plan: SparkPlan): Unit = {
//      println(s"findQueryStages: ${plan.getClass} with ${plan.children.length} children")
      plan match {
        case p: AdaptiveSparkPlanExec => findQueryStages(p.executedPlan)
        case p: QueryStageExec =>
          stages += p
          findQueryStages(p.plan)
        case p: AQEShuffleReadExec => findQueryStages(p.child)
        case _ => plan.children.foreach(findQueryStages)
      }
    }
    findQueryStages(plan)
//    println(s"found ${stages.length} query stages")

    println(s"Writing $filename")
    val w = new BufferedWriter(new FileWriter(filename))
    w.write("digraph G {\n")
    for (stage <- stages) {
      generateQueryStage(w, stage)
    }

    // generate links
    def generateLinks(plan: SparkPlan, shuffleReadId: Option[Int]): Unit = {
//      println(s"generateLinks: ${plan.getClass} id=${plan.id} shuffleReadId=$shuffleReadId")

      plan match {
        case p: AdaptiveSparkPlanExec =>
          generateLinks(p.executedPlan, shuffleReadId)

        case p: QueryStageExec =>
          if (shuffleReadId.isDefined) {
            w.write(s"\tnode_${p.id} -> node_${shuffleReadId.get};\n")
          }
          generateLinks(p.plan, shuffleReadId)

        case p: AQEShuffleReadExec =>
          generateLinks(p.child, Some(p.id))

        case _ => plan.children.foreach(x => {
          generateLinks(x, shuffleReadId)
        })
      }
    }
    w.write("// links between query stages\n")
    generateLinks(plan, None)

    // generate the final part of the plan
    w.write("// final part of plan\n")
    generate(w, plan)



    w.write("}\n")
    w.close()
  }

  def generate(w: BufferedWriter, plan: SparkPlan): Unit = {
    println(s"SparkDot.generate: ${plan.getClass}")

    // TODO subqueries in projections and filters

    plan match {
      case p: FileSourceScanExec =>
        val filename = p.relation.location.inputFiles.head
        val path = filename.substring(0, filename.lastIndexOf('/'))
        w.write(s"// ${plan.simpleStringWithNodeId()}\n")
        w.write(s"""\tnode_${plan.id} [shape=box, label = "FileSourceScanExec: $path"];\n""")

      case p: InputAdapter =>
        w.write(s"""\tnode_${p.id} [shape=box, label = "InputAdapter"];\n""")
        generate(w, p.child)
        w.write(s"\tnode_${p.child.id} -> node_${p.id};\n")

      case p: AQEShuffleReadExec =>
        w.write(s"// ${plan.simpleStringWithNodeId()}\n")
        w.write(s"""\tnode_${plan.id} [shape=box, label = "AQEShuffleReadExec: child.id = ${p.child.id}"];\n""")

      case _ =>
        w.write(s"// ${plan.simpleStringWithNodeId()}\n")
        w.write(s"""\tnode_${plan.id} [shape=box, label = "${plan.nodeName}"];\n""")
        children(plan).foreach(ch => {
          generate(w, ch)
          w.write(s"\tnode_${ch.id} -> node_${plan.id};\n")
        })
    }
  }

  /**
   * Write plan for one query stage. Do not recurse into other query stages.
   */
  private def generateQueryStage(w: BufferedWriter, queryStage: QueryStageExec): Unit = {
    val clusterId = nextCluster
    nextCluster += 1
    w.write(s"// Query Stage id=${queryStage.id}; ${queryStage.simpleStringWithNodeId()}\n")
    w.write(s"subgraph cluster$clusterId {\n")
    val label = s"${queryStage.nodeName}\nThis stage produced ${queryStage.getRuntimeStatistics.rowCount.getOrElse(-1)} rows."
    w.write(s"""label = "$label";\n""")
    generate(w, queryStage.plan)

    w.write(s"\tnode_${queryStage.plan.id} -> node_${queryStage.id};\n")

    w.write(s"}\n\n")
  }

  def children(plan: SparkPlan): Seq[SparkPlan] = {
    plan match {
      case p: AdaptiveSparkPlanExec => Seq(p.executedPlan)
      case _ => plan.children
    }
  }

}

