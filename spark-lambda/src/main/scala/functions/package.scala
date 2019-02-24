import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.ActivityByProduct
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka.HasOffsetRanges
import domain.{Activity, ActivityByProduct}

/**
  * @author behzad.pirvali, 9/29/18
  */
package object functions {
  def rddToRDDActivity(input: RDD[(String, String)]) = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex( {(index, it) =>
      val or = offsetRanges(index)
      it.flatMap { kv =>
        val line = kv._2
        val record = line.split("\\t")
        val MS_IN_HOUR = 1000 * 60 * 60
        if (record.length == 7)
          Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6),
            Map("topic"-> or.topic, "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }


  def mapActivityStateFunc = (k: (String, Long), value: Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {
    var (purchase_count, add_to_cart_count, page_view_count) =
      state.getOption().getOrElse(0L, 0L, 0L)

    val v = value.getOrElse(ActivityByProduct("", 0L, 0L, 0L, 0L ))

    purchase_count += v.purchase_count
    add_to_cart_count += v.add_to_cart_count
    page_view_count += v.page_view_count
    state.update((purchase_count, add_to_cart_count, page_view_count))

    val underExposed =
      if (purchase_count == 0)
        0
      else
        page_view_count / purchase_count

    underExposed
  }

  def mapVisitorsStateFunc = (k: (String, Long), v: Option[HLL], state: State[HLL]) => {
    val currentVisitorHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    val newVisitorHLL = v match {
      case Some(visitorHLL) => currentVisitorHLL + visitorHLL
      case None => currentVisitorHLL
    }
    state.update(newVisitorHLL)
    val output = newVisitorHLL.approximateSize.estimate
    output
  }

}
