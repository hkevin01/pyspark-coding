/**
 * GraphFrames Example in Scala
 * =============================
 * 
 * Demonstrates graph processing with GraphFrames in Apache Spark.
 * 
 * GraphFrames provides:
 * - Graph queries using DataFrame API
 * - Graph algorithms (PageRank, Connected Components, Triangle Counting)
 * - Motif finding (pattern matching in graphs)
 * - Subgraph creation and filtering
 * 
 * Real-world applications:
 * - Social network analysis
 * - Fraud detection in transaction networks
 * - Recommendation systems
 * - Knowledge graphs
 * - Biological network analysis
 * 
 * Installation:
 * Add to build.sbt or provide as package:
 * libraryDependencies += "graphframes" % "graphframes" % "0.8.2-spark3.2-s_2.12"
 * 
 * Or run with:
 * spark-shell --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12
 */

// Import necessary libraries
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

// Create a Spark session
val spark = SparkSession.builder
  .appName("GraphFramesExample")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

println("=" * 70)
println("GRAPHFRAMES EXAMPLE - SOCIAL NETWORK ANALYSIS")
println("=" * 70)

// ============================================================================
// STEP 1: Define vertices (nodes) and edges (relationships)
// ============================================================================

println("\n📊 STEP 1: Creating Graph Structure")
println("-" * 70)

// Define vertices (people in a social network)
val vertices = spark.createDataFrame(Seq(
  (1L, "Scott", 30),
  (2L, "David", 40),
  (3L, "Mike", 45),
  (4L, "Alice", 35),
  (5L, "Bob", 28)
)).toDF("id", "name", "age")

// Define edges (relationships between people)
val edges = spark.createDataFrame(Seq(
  (1L, 2L, "friend"),
  (2L, 3L, "follow"),
  (1L, 4L, "friend"),
  (4L, 5L, "friend"),
  (3L, 1L, "follow"),
  (5L, 2L, "follow")
)).toDF("src", "dst", "relationship")

// Create a GraphFrame
val graph = GraphFrame(vertices, edges)

println("\n👥 Vertices (People):")
graph.vertices.show()

println("\n🔗 Edges (Relationships):")
graph.edges.show()

// ============================================================================
// STEP 2: Basic Graph Queries
// ============================================================================

println("\n🔍 STEP 2: Graph Queries")
println("-" * 70)

// Find Scott's friends (id = 1)
println("\n👫 Scott's connections:")
val scottConnections = graph.edges
  .filter("src = 1")
  .join(graph.vertices, $"dst" === $"id")
  .select("dst", "name", "relationship")
scottConnections.show()

// Find all friend relationships
println("\n💫 All friend relationships:")
val friendships = graph.edges.filter("relationship = 'friend'")
friendships.show()

// ============================================================================
// STEP 3: Graph Analytics - Degree Metrics
// ============================================================================

println("\n📈 STEP 3: Graph Analytics - Degree Metrics")
println("-" * 70)

// In-degrees (how many people follow/friend this person)
println("\n📥 In-Degrees (incoming connections):")
val inDegrees = graph.inDegrees
inDegrees.orderBy($"inDegree".desc).show()

// Out-degrees (how many people this person follows/friends)
println("\n📤 Out-Degrees (outgoing connections):")
val outDegrees = graph.outDegrees
outDegrees.orderBy($"outDegree".desc).show()

// Total degrees
println("\n🔄 Total Degrees (all connections):")
val degrees = graph.degrees
degrees.orderBy($"degree".desc).show()

// ============================================================================
// STEP 4: Subgraph Creation
// ============================================================================

println("\n🎯 STEP 4: Subgraph Filtering")
println("-" * 70)

// Create subgraph of people aged 40 or older with friend relationships
println("\n👴 Subgraph: People aged >= 40, friend relationships only:")
val subgraph = graph
  .filterVertices("age >= 40")
  .filterEdges("relationship = 'friend'")

subgraph.vertices.show()
subgraph.edges.show()

// ============================================================================
// STEP 5: Graph Algorithms - PageRank
// ============================================================================

println("\n🏆 STEP 5: PageRank Algorithm")
println("-" * 70)

// Run PageRank to find influential people in the network
println("\n⭐ PageRank Results (measures influence/importance):")
val pageRankResults = graph.pageRank
  .resetProbability(0.15)
  .maxIter(10)
  .run()

println("\nVertex Rankings:")
pageRankResults.vertices
  .orderBy($"pagerank".desc)
  .show()

println("\nEdge Weights:")
pageRankResults.edges.show()

// ============================================================================
// STEP 6: Connected Components
// ============================================================================

println("\n🌐 STEP 6: Connected Components")
println("-" * 70)

// Find connected components (groups of connected people)
val connectedComponents = graph.connectedComponents.run()
println("\n🔗 Connected groups in the network:")
connectedComponents
  .orderBy("component")
  .show()

// ============================================================================
// STEP 7: Triangle Count
// ============================================================================

println("\n🔺 STEP 7: Triangle Counting")
println("-" * 70)

// Count triangles (groups of 3 mutually connected people)
println("\n📐 Triangle counts (measures clustering):")
val triangleCounts = graph.triangleCount.run()
triangleCounts
  .orderBy($"count".desc)
  .show()

// ============================================================================
// STEP 8: Motif Finding (Pattern Matching)
// ============================================================================

println("\n🔎 STEP 8: Motif Finding (Pattern Matching)")
println("-" * 70)

// Find motifs: A follows B, B follows C
println("\n➡️  Pattern: (a)-[]->(b); (b)-[]->(c)")
println("   Finding follow chains...")
val motifs = graph.find("(a)-[e1]->(b); (b)-[e2]->(c)")
motifs.show()

// Find mutual friendships: A friends B, B friends A
println("\n🤝 Pattern: Mutual relationships")
val mutualFriends = graph.find("(a)-[e1]->(b); (b)-[e2]->(a)")
  .filter("a.id < b.id")  // Avoid duplicates
mutualFriends.show()

// ============================================================================
// STEP 9: Shortest Paths
// ============================================================================

println("\n🛣️  STEP 9: Shortest Paths")
println("-" * 70)

// Find shortest paths from Scott (id=1) to all other vertices
println("\n📍 Shortest paths from Scott (id=1):")
val shortestPaths = graph.shortestPaths.landmarks(Seq(1L, 3L, 5L)).run()
shortestPaths.show()

// ============================================================================
// SUMMARY
// ============================================================================

println("\n" + "=" * 70)
println("SUMMARY - GRAPHFRAMES CAPABILITIES")
println("=" * 70)
println("""
✅ Graph Structure:
   • Vertices (nodes): People with attributes
   • Edges (relationships): Connections with types

✅ Graph Queries:
   • Filter vertices and edges
   • Join graph data with DataFrame operations
   • Create subgraphs

✅ Graph Analytics:
   • Degree metrics (in-degree, out-degree, total degree)
   • PageRank (influence/importance)
   • Connected components (find groups)
   • Triangle counting (clustering coefficient)

✅ Pattern Matching:
   • Motif finding (complex graph patterns)
   • Shortest paths algorithms
   • BFS (Breadth-First Search)

✅ Real-World Applications:
   • Social Network Analysis (Facebook, LinkedIn)
   • Fraud Detection (transaction networks)
   • Recommendation Systems (collaborative filtering)
   • Knowledge Graphs (Google Knowledge Graph)
   • Biological Networks (protein interactions)

✅ Performance:
   • Distributed processing on Spark
   • Scales to billions of vertices and edges
   • Leverages DataFrame optimizations
""")

println("=" * 70)

// Stop the Spark session
spark.stop()
