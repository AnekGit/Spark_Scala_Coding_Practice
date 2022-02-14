class Graph(path :String){
  println(s"reading file from path")

  def getEdges = 256

  def getVertices = 12

  def persist(int : Int ) = {
    println(s" persist the file ${int}")
  }


}
object Graph{

  val MEMORY_ONLY = 1
  val MEMORY_AND_DISK = 2

}
val graph = new Graph("PATH")
graph.persist(Graph.MEMORY_AND_DISK)

//graph.path

List(1,2,3)
List.apply(1,2,3)