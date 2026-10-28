object ScRNAPipeline {
  def main(args: Array[String]) : Unit = {
    if (args.length < 6) {
      println("Usage: program.jar -i input_file -o output_file -m local|spark -t dense|sparse")
      sys.exit(-1)
    }
    val config: Map[String, String] = nextArg(args.toList, Map())

    if (config("mode") == "local") {
      local.Pipeline.run(config)
    } else {
      distributed.sparkvector.Pipeline.run(config)
    }

  }

  def nextArg(args: List[String], map: Map[String, String]) : Map[String,String] = {
    args match {
      case "-i" :: value :: tail => {
        nextArg(tail, map ++ Map("input" -> value))
      }
      case "-o" :: value :: tail => {
        nextArg(tail, map ++ Map("output" -> value))
      }
      case "-m" :: value :: tail => {
        nextArg(tail, map ++ Map("mode" -> value))
      }
      case "-t" :: value :: tail => {
        nextArg(tail, map ++ Map("type" -> value))
      }
      case Nil => map
      case default => {
        println(s"Unknown option: ${default}")
        sys.exit(-1)
      }
    }
  }
}
