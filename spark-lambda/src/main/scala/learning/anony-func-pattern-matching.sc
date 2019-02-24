val wordFrequencies = ("habitual", 6) :: ("and", 56) :: ("consuetudinary", 2) ::
  ("additionally", 27) :: ("homely", 5) :: ("society", 13) :: Nil

//def wordsWithoutOutliers(wordFrequencies: Seq[(String, Int)]): Seq[String] =
//  wordFrequencies.filter(wf => wf._2 > 3 && wf._2 < 25).map(_._1)
def wordsWithoutOutliers(wordFrequencies: Seq[(String, Int)]): Seq[String] =
  wordFrequencies.filter { case (w, f) => f > 3 && f < 25 }.map { case (w, f) => w }
wordsWithoutOutliers(wordFrequencies)