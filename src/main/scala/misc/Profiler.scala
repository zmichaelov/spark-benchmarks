package misc

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskStart
import scala.collection.mutable

/**
 * Created by zmichaelov on 3/2/14.
 */
class Profiler extends SparkListener {
  private val jobs = new mutable.HashMap[Int, Long];// map jobids to runtimes

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = super.onTaskGettingResult(taskGettingResult)

  override def onJobStart(jobStart: SparkListenerJobStart){ //Unit = super.onJobStart(jobStart)
    val activeJob = jobStart.job;
    jobs.put(activeJob.jobId, System.currentTimeMillis());

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val finishedJob = jobEnd.job;
    val startTime = jobs(finishedJob.jobId);
    jobs.update(finishedJob.jobId, System.currentTimeMillis() - startTime);
  }//: Unit = super.onJobEnd(jobEnd)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
//   taskEnd.taskInfo.
  }//: Unit = super.onTaskEnd(taskEnd)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted)//: Unit = super.onStageSubmitted(stageSubmitted)
  {
    //stageSubmitted.stage.
  }
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted)//: Unit = super.onStageCompleted(stageCompleted)
  {
   val stageInfo = stageCompleted.stage;
    stageInfo.completionTime
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = super.onTaskStart(taskStart)
}
