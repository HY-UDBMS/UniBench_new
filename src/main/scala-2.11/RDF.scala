import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDF {
  def Create(spark: SparkSession): Unit = {
    val TOTAL_LEVELS = 2

    var previous_nodes: Set[String] = null
    var current_triples: RDD[graph.Triple] = null

    var sample = spark.sparkContext.emptyRDD[graph.Triple]

    for (level <- 0 until TOTAL_LEVELS) {
      current_triples = spark.rdf(Lang.NTRIPLES)(s"src/main/resources/dbpedia/level$level.nt.*")

      var p = 0.01d

      val collected = collect(spark, p, previous_nodes, current_triples)
      println(s"$level: ${collected.count()}")

      sample = sample.union(collected)

      previous_nodes = sample.getObjects().filter(_.isURI).map(_.getURI).collect.toSet
    }

    sample
      .map(new JenaTripleToNTripleString)
      .repartition(1)
      .saveAsTextFile(spark.conf.get("rdf"))
  }

  private def collect(spark: SparkSession, p: Double, previous_nodes: Set[String], current_triples: RDD[graph.Triple]): RDD[graph.Triple] = {
    if (previous_nodes == null)
      return current_triples

    val b_previous_nodes = spark.sparkContext.broadcast(previous_nodes)

    current_triples.filterSubjects(s => b_previous_nodes.value.contains(s.getURI))
  }


  class JenaTripleToNTripleString
    extends (Triple => String)
      with java.io.Serializable {
    override def apply(t: Triple): String = {
      val objStr =
        if (t.getObject.isLiteral) {
          t.getObject.toString.replace("\n", "\\n")
        } else {
          s"<${t.getObject}>"
        }
      s"<${t.getSubject}> <${t.getPredicate}> ${objStr} ."
    }
  }

}