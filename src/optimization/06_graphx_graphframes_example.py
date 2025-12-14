"""
Spark GraphX and GraphFrames Example

This module demonstrates graph processing in Apache Spark:
• GraphX (RDD-based, legacy, pre-Spark 3.0)
• GraphFrames (DataFrame-based, Spark 3.0+, recommended)

GraphFrames extends Spark's DataFrame API to support graph operations,
providing high-level abstractions for graph analytics with all DataFrame
capabilities (SQL, optimizations, DataSource API).

GRAPHX vs GRAPHFRAMES:
=====================
GraphX (Pre-Spark 3.0):
• RDD-based API
• Low-level operations
• Lost DataFrame benefits (optimizations, SQL)
• Legacy support only

GraphFrames (Spark 3.0+):
• DataFrame-based API
• High-level abstractions
• Full DataFrame capabilities
• SQL queries on graphs
• Better performance
• Recommended for all new projects

Author: PySpark Learning Series
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_graphframes_spark_session():
    """
    Create Spark session with GraphFrames support.
    
    GRAPHFRAMES OVERVIEW:
    ====================
    GraphFrames is a graph processing library built on DataFrames that provides:
    • High-level graph abstractions
    • DataFrame-based API
    • Graph algorithms (PageRank, connected components, etc.)
    • Motif finding (pattern matching)
    • Integration with Spark SQL
    
    INSTALLATION:
    ============
    # Option 1: Install via pip
    pip install graphframes
    
    # Option 2: Add package when submitting Spark job
    spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 script.py
    
    # Option 3: Add to SparkSession
    spark = SparkSession.builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .getOrCreate()
    
    GRAPH STRUCTURE:
    ===============
    A graph consists of two DataFrames:
    
    1. VERTICES (Nodes):
       DataFrame with:
       • "id" column (required)
       • Additional attributes (optional)
       
       Example:
       +---+-----+---+
       |id |name |age|
       +---+-----+---+
       |1  |Alice|30 |
       |2  |Bob  |35 |
       +---+-----+---+
    
    2. EDGES (Relationships):
       DataFrame with:
       • "src" column (source vertex id)
       • "dst" column (destination vertex id)
       • Additional attributes (optional)
       
       Example:
       +---+---+------------+
       |src|dst|relationship|
       +---+---+------------+
       |1  |2  |friend      |
       |2  |3  |colleague   |
       +---+---+------------+
    
    Returns:
        SparkSession configured for GraphFrames
    """
    print("\n" + "="*70)
    print("CREATING SPARK SESSION FOR GRAPHFRAMES")
    print("="*70)
    
    spark = SparkSession.builder \
        .appName("GraphX and GraphFrames Example") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("\n✅ Spark session created")
    print(f"   Spark version: {spark.version}")
    
    print("\n📦 GraphFrames Installation:")
    print("   For production use, install via:")
    print("   pip install graphframes")
    print("   OR")
    print("   spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12")
    
    return spark


def example_1_basic_graph_creation(spark):
    """
    Create a basic graph and explore its structure.
    
    GRAPH CONCEPTS:
    ==============
    
    VERTICES (Nodes):
    ----------------
    Represent entities in the graph.
    Examples: People, cities, products, web pages
    
    EDGES (Links/Relationships):
    ---------------------------
    Represent connections between vertices.
    Examples: Friendships, routes, purchases, hyperlinks
    
    DIRECTED vs UNDIRECTED:
    ----------------------
    Directed: Edge has direction (A → B ≠ B → A)
              Example: Twitter follows, web links
    
    Undirected: Edge has no direction (A ↔ B)
                Example: Facebook friends, co-authors
    
    REAL-WORLD GRAPH EXAMPLE:
    ========================
    Social Network:
    
         Alice
        ↙     ↘
      Bob  ←  Charlie
       ↓        ↓
     Diana  →  Eve
    
    Vertices: 5 people
    Edges: 6 relationships
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic Graph Creation")
    print("="*70)
    
    # Note: In production, import graphframes
    # from graphframes import GraphFrame
    
    print("\n📊 Creating Social Network Graph...")
    
    # Create vertices DataFrame (people)
    vertices = spark.createDataFrame([
        (1, "Alice", 30, "Engineer"),
        (2, "Bob", 35, "Manager"),
        (3, "Charlie", 28, "Analyst"),
        (4, "Diana", 32, "Designer"),
        (5, "Eve", 29, "Developer")
    ], ["id", "name", "age", "role"])
    
    print("\n   VERTICES (People):")
    vertices.show()
    
    # Create edges DataFrame (relationships)
    edges = spark.createDataFrame([
        (1, 2, "friend"),      # Alice → Bob
        (1, 3, "friend"),      # Alice → Charlie
        (2, 4, "colleague"),   # Bob → Diana
        (3, 2, "friend"),      # Charlie → Bob
        (3, 5, "colleague"),   # Charlie → Eve
        (4, 5, "friend")       # Diana → Eve
    ], ["src", "dst", "relationship"])
    
    print("\n   EDGES (Relationships):")
    edges.show()
    
    # Graph structure visualization
    print("\n   GRAPH VISUALIZATION:")
    print("""
         Alice (1)
        ↙friend   ↘friend
      Bob (2)  ←  Charlie (3)
       ↓colleague   ↓colleague
     Diana (4)  →  Eve (5)
              friend
    """)
    
    # Basic graph statistics
    print("\n📈 GRAPH STATISTICS:")
    print(f"   Vertices: {vertices.count()}")
    print(f"   Edges: {edges.count()}")
    
    # In production with GraphFrames:
    print("\n💡 WITH GRAPHFRAMES LIBRARY:")
    print("""
   from graphframes import GraphFrame
   
   # Create graph
   g = GraphFrame(vertices, edges)
   
   # Basic operations
   print(f"Vertices: {g.vertices.count()}")
   print(f"Edges: {g.edges.count()}")
   
   # Get degrees (number of connections)
   g.degrees.show()
   g.inDegrees.show()   # Incoming edges
   g.outDegrees.show()  # Outgoing edges
    """)
    
    # Calculate degrees manually (without GraphFrames)
    print("\n🔢 CALCULATING DEGREES (connections per person):")
    
    # Out-degree: edges leaving vertex
    out_degree = edges.groupBy("src") \
        .agg(count("*").alias("out_degree")) \
        .withColumnRenamed("src", "id")
    
    # In-degree: edges entering vertex
    in_degree = edges.groupBy("dst") \
        .agg(count("*").alias("in_degree")) \
        .withColumnRenamed("dst", "id")
    
    # Total degree
    degrees = vertices.select("id", "name") \
        .join(out_degree, "id", "left") \
        .join(in_degree, "id", "left") \
        .fillna(0) \
        .withColumn("total_degree", col("out_degree") + col("in_degree")) \
        .orderBy(desc("total_degree"))
    
    print("\n   Degree Analysis:")
    degrees.show()
    
    print("\n   Interpretation:")
    print("   • out_degree: Number of connections initiated")
    print("   • in_degree: Number of connections received")
    print("   • total_degree: Total connections")
    print("   • Higher degree = More connected person")


def example_2_graph_algorithms(spark):
    """
    Demonstrate common graph algorithms.
    
    COMMON GRAPH ALGORITHMS:
    =======================
    
    1. PAGERANK:
       • Measures importance of vertices
       • Used by Google for web page ranking
       • Vertices with many incoming edges = high rank
    
    2. CONNECTED COMPONENTS:
       • Find groups of connected vertices
       • Useful for community detection
       • Example: Friend circles in social network
    
    3. TRIANGLE COUNT:
       • Count triangles each vertex participates in
       • Measures clustering coefficient
       • Example: Mutual friends (A→B→C→A)
    
    4. SHORTEST PATHS:
       • Find shortest path between vertices
       • Breadth-first search (BFS)
       • Example: Degrees of separation
    
    5. LABEL PROPAGATION:
       • Community detection algorithm
       • Vertices adopt labels from neighbors
       • Converges to communities
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: Graph Algorithms")
    print("="*70)
    
    # Create larger graph for algorithms
    print("\n📊 Creating Larger Social Network...")
    
    vertices = spark.createDataFrame([
        (1, "Alice"), (2, "Bob"), (3, "Charlie"),
        (4, "Diana"), (5, "Eve"), (6, "Frank"),
        (7, "Grace"), (8, "Henry"), (9, "Iris")
    ], ["id", "name"])
    
    edges = spark.createDataFrame([
        (1, 2), (1, 3), (2, 3), (2, 4),
        (3, 4), (3, 5), (4, 5), (5, 6),
        (6, 7), (6, 8), (7, 8), (7, 9), (8, 9)
    ], ["src", "dst"])
    
    print(f"\n   Vertices: {vertices.count()}")
    print(f"   Edges: {edges.count()}")
    
    # Visualize graph structure
    print("\n   GRAPH STRUCTURE:")
    print("""
   Group 1:                Group 2:              Group 3:
   Alice ← → Bob           Eve ← → Frank         Grace
     ↓  ×  ↓                              ↙  ↘     ↘
   Charlie → Diana                  Henry  ←  →  Iris
    
   (← → indicates bidirectional edges)
    """)
    
    print("\n" + "="*70)
    print("ALGORITHM 1: PageRank (Importance Ranking)")
    print("="*70)
    
    print("\n💡 PAGERANK CONCEPT:")
    print("""
   • Iterative algorithm
   • Importance = sum of importance of incoming neighbors
   • Vertices with many/important incoming edges = high rank
   
   Formula:
   PR(v) = 0.15 + 0.85 × Σ(PR(u) / out_degree(u))
           where u → v (u points to v)
   
   Applications:
   • Web page ranking (Google)
   • Influential people in social networks
   • Important papers in citation networks
    """)
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   from graphframes import GraphFrame
   g = GraphFrame(vertices, edges)
   
   # Run PageRank
   results = g.pageRank(resetProbability=0.15, maxIter=10)
   results.vertices.select("id", "name", "pagerank").show()
    """)
    
    print("\n" + "="*70)
    print("ALGORITHM 2: Connected Components (Community Detection)")
    print("="*70)
    
    print("\n💡 CONNECTED COMPONENTS CONCEPT:")
    print("""
   • Find groups where all vertices are reachable
   • Each component = isolated subgraph
   • Useful for finding communities, clusters
   
   Example:
   If A→B→C and D→E, but no path from ABC to DE:
   • Component 1: {A, B, C}
   • Component 2: {D, E}
   
   Applications:
   • Social network communities
   • Network topology analysis
   • Fraud detection (connected accounts)
    """)
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Find connected components
   result = g.connectedComponents()
   result.select("id", "name", "component").show()
   
   # Count vertices per component
   result.groupBy("component").count().show()
    """)
    
    print("\n" + "="*70)
    print("ALGORITHM 3: Triangle Count (Clustering)")
    print("="*70)
    
    print("\n💡 TRIANGLE COUNT CONCEPT:")
    print("""
   • Count triangles each vertex participates in
   • Triangle: A→B→C→A (3 mutual connections)
   • Measures local clustering
   
   Example:
   Alice → Bob → Charlie → Alice
   This is 1 triangle involving all 3 vertices
   
   High triangle count = Tight-knit community
   
   Applications:
   • Social network cohesion
   • Spam detection (real users have triangles)
   • Recommendation systems
    """)
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Count triangles
   result = g.triangleCount()
   result.select("id", "name", "count").show()
    """)
    
    print("\n" + "="*70)
    print("ALGORITHM 4: Shortest Paths (BFS)")
    print("="*70)
    
    print("\n💡 SHORTEST PATHS CONCEPT:")
    print("""
   • Find shortest path between two vertices
   • Uses Breadth-First Search (BFS)
   • Returns path length (number of hops)
   
   Example:
   Path from Alice to Eve:
   Alice → Charlie → Eve (2 hops)
   Better than: Alice → Bob → Diana → Eve (3 hops)
   
   Applications:
   • Degrees of separation
   • Navigation systems
   • Network routing
    """)
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Find shortest paths from Alice to all vertices
   paths = g.shortestPaths(landmarks=["1"])  # Alice's id
   paths.select("id", "name", "distances").show()
   
   # BFS to find path with conditions
   from graphframes.lib import AggregateMessages as AM
   
   # Find vertices 2 hops from Alice
   result = g.bfs("id = 1", "id = 5", maxPathLength=3)
   result.show()
    """)


def example_3_motif_finding(spark):
    """
    Demonstrate motif finding (pattern matching in graphs).
    
    MOTIF FINDING:
    =============
    Find patterns (motifs) in graphs using SQL-like syntax.
    
    MOTIF PATTERNS:
    ==============
    • "(a)-[e]->(b)": Edge from a to b
    • "(a)-[e1]->(b); (b)-[e2]->(c)": Path a→b→c
    • "(a)-[e1]->(b); (b)-[e2]->(a)": Bidirectional edge
    • "(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)": Triangle
    
    Variables:
    • (a), (b), (c): Vertices
    • [e], [e1], [e2]: Edges
    
    EXAMPLE PATTERNS:
    ================
    1. Friends of friends:
       "(a)-[e1]->(b); (b)-[e2]->(c)"
    
    2. Mutual friends:
       "(a)-[e1]->(b); (b)-[e2]->(a)"
    
    3. Triangles:
       "(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)"
    
    4. Squares:
       "(a)-[]->(b); (b)-[]->(c); (c)-[]->(d); (d)-[]->(a)"
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Motif Finding (Pattern Matching)")
    print("="*70)
    
    print("\n📊 Creating Graph with Relationship Types...")
    
    vertices = spark.createDataFrame([
        (1, "Alice"), (2, "Bob"), (3, "Charlie"),
        (4, "Diana"), (5, "Eve")
    ], ["id", "name"])
    
    edges = spark.createDataFrame([
        (1, 2, "friend"),
        (2, 1, "friend"),      # Mutual friendship
        (1, 3, "colleague"),
        (2, 4, "friend"),
        (3, 5, "colleague"),
        (4, 5, "friend")
    ], ["src", "dst", "relationship"])
    
    print("\n   VERTICES:")
    vertices.show()
    
    print("\n   EDGES:")
    edges.show()
    
    print("\n" + "="*70)
    print("MOTIF PATTERN 1: Friends of Friends")
    print("="*70)
    
    print("\n💡 PATTERN: (a)-[e1]->(b); (b)-[e2]->(c)")
    print("   Find: a knows b, and b knows c")
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Find friends of friends
   motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
   motifs.select("a.name", "b.name", "c.name").show()
   
   # Filter to only 'friend' relationships
   motifs.filter("e1.relationship = 'friend' AND e2.relationship = 'friend'") \\
       .select("a.name", "b.name", "c.name") \\
       .show()
    """)
    
    # Manual implementation (without GraphFrames)
    print("\n   MANUAL IMPLEMENTATION:")
    friends_of_friends = edges.alias("e1") \
        .join(edges.alias("e2"), col("e1.dst") == col("e2.src")) \
        .join(vertices.alias("a"), col("e1.src") == col("a.id")) \
        .join(vertices.alias("b"), col("e1.dst") == col("b.id")) \
        .join(vertices.alias("c"), col("e2.dst") == col("c.id")) \
        .select(
            col("a.name").alias("person_a"),
            col("b.name").alias("person_b"),
            col("c.name").alias("person_c")
        )
    
    print("\n   Friends of Friends:")
    friends_of_friends.show(10)
    
    print("\n" + "="*70)
    print("MOTIF PATTERN 2: Mutual Friendships")
    print("="*70)
    
    print("\n💡 PATTERN: (a)-[e1]->(b); (b)-[e2]->(a)")
    print("   Find: a→b AND b→a (bidirectional)")
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Find mutual friendships
   mutual = g.find("(a)-[e1]->(b); (b)-[e2]->(a)")
   mutual.filter("a.id < b.id")  # Avoid duplicates
       .select("a.name", "b.name")
       .show()
    """)
    
    # Manual implementation
    print("\n   MANUAL IMPLEMENTATION:")
    mutual_friends = edges.alias("e1") \
        .join(
            edges.alias("e2"),
            (col("e1.src") == col("e2.dst")) & (col("e1.dst") == col("e2.src"))
        ) \
        .filter(col("e1.src") < col("e1.dst")) \
        .join(vertices.alias("a"), col("e1.src") == col("a.id")) \
        .join(vertices.alias("b"), col("e1.dst") == col("b.id")) \
        .select(
            col("a.name").alias("person_1"),
            col("b.name").alias("person_2")
        )
    
    print("\n   Mutual Friendships:")
    mutual_friends.show()
    
    print("\n" + "="*70)
    print("MOTIF PATTERN 3: Triangles")
    print("="*70)
    
    print("\n💡 PATTERN: (a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
    print("   Find: a→b→c→a (circular)")
    
    print("\n   WITH GRAPHFRAMES:")
    print("""
   # Find all triangles
   triangles = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
   triangles.select("a.name", "b.name", "c.name").show()
    """)
    
    print("\n💡 APPLICATIONS:")
    print("""
   1. RECOMMENDATION SYSTEMS:
      • Friends of friends suggestions
      • "People you may know"
   
   2. FRAUD DETECTION:
      • Detect suspicious patterns
      • Find circular transaction networks
   
   3. SOCIAL NETWORK ANALYSIS:
      • Find communities (triangles)
      • Identify influencers (central nodes)
   
   4. KNOWLEDGE GRAPHS:
      • Find relationships between entities
      • Query complex patterns
    """)


def example_4_graph_queries_with_sql(spark):
    """
    Demonstrate SQL queries on graphs using DataFrames.
    
    ADVANTAGE OF GRAPHFRAMES:
    ========================
    Since graphs are DataFrames, you can:
    • Use SQL queries
    • Apply DataFrame operations
    • Join with other DataFrames
    • Use built-in functions
    • Leverage Spark optimizations
    
    This was NOT possible with GraphX (RDD-based).
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: Graph Queries with SQL")
    print("="*70)
    
    print("\n📊 Creating Employee Collaboration Network...")
    
    # Vertices: Employees
    vertices = spark.createDataFrame([
        (1, "Alice", "Engineering", 50000),
        (2, "Bob", "Engineering", 60000),
        (3, "Charlie", "Sales", 45000),
        (4, "Diana", "Marketing", 48000),
        (5, "Eve", "Engineering", 55000),
        (6, "Frank", "Sales", 47000)
    ], ["id", "name", "department", "salary"])
    
    # Edges: Collaborations
    edges = spark.createDataFrame([
        (1, 2, "mentor", 10),
        (1, 5, "colleague", 5),
        (2, 3, "cross-team", 3),
        (3, 4, "colleague", 7),
        (3, 6, "colleague", 8),
        (4, 5, "cross-team", 2)
    ], ["src", "dst", "type", "projects_together"])
    
    print("\n   EMPLOYEES:")
    vertices.show()
    
    print("\n   COLLABORATIONS:")
    edges.show()
    
    # Register as temp views for SQL
    vertices.createOrReplaceTempView("employees")
    edges.createOrReplaceTempView("collaborations")
    
    print("\n" + "="*70)
    print("SQL QUERY 1: High Collaboration Pairs")
    print("="*70)
    
    query1 = """
        SELECT 
            e1.name as person_1,
            e2.name as person_2,
            c.type as collaboration_type,
            c.projects_together
        FROM collaborations c
        JOIN employees e1 ON c.src = e1.id
        JOIN employees e2 ON c.dst = e2.id
        WHERE c.projects_together >= 5
        ORDER BY c.projects_together DESC
    """
    
    print(f"\n   Query:\n{query1}")
    result1 = spark.sql(query1)
    result1.show()
    
    print("\n" + "="*70)
    print("SQL QUERY 2: Cross-Department Collaborations")
    print("="*70)
    
    query2 = """
        SELECT 
            e1.name as person_1,
            e1.department as dept_1,
            e2.name as person_2,
            e2.department as dept_2,
            c.projects_together
        FROM collaborations c
        JOIN employees e1 ON c.src = e1.id
        JOIN employees e2 ON c.dst = e2.id
        WHERE e1.department != e2.department
        ORDER BY c.projects_together DESC
    """
    
    print(f"\n   Query:\n{query2}")
    result2 = spark.sql(query2)
    result2.show()
    
    print("\n" + "="*70)
    print("SQL QUERY 3: Department Collaboration Summary")
    print("="*70)
    
    query3 = """
        SELECT 
            e1.department,
            COUNT(*) as collaboration_count,
            AVG(c.projects_together) as avg_projects,
            SUM(c.projects_together) as total_projects
        FROM collaborations c
        JOIN employees e1 ON c.src = e1.id
        GROUP BY e1.department
        ORDER BY total_projects DESC
    """
    
    print(f"\n   Query:\n{query3}")
    result3 = spark.sql(query3)
    result3.show()
    
    print("\n💡 KEY ADVANTAGE:")
    print("""
   GraphFrames = DataFrames = SQL Support
   
   You can:
   ✅ Write SQL queries on graphs
   ✅ Use DataFrame operations (filter, groupBy, join)
   ✅ Apply built-in functions (avg, sum, count)
   ✅ Join graphs with other DataFrames
   ✅ Leverage Catalyst optimizer
   ✅ Use DataSource API (read from any source)
   
   GraphX (RDD-based) could NOT do this!
    """)


def example_5_real_world_use_cases(spark):
    """
    Demonstrate real-world graph processing use cases.
    
    REAL-WORLD APPLICATIONS:
    =======================
    
    1. SOCIAL NETWORKS:
       • Friend recommendations
       • Influencer identification
       • Community detection
       • Spam detection
    
    2. RECOMMENDATION SYSTEMS:
       • Product recommendations
       • Content suggestions
       • Collaborative filtering
    
    3. FRAUD DETECTION:
       • Suspicious transaction patterns
       • Account networks
       • Identity verification
    
    4. KNOWLEDGE GRAPHS:
       • Entity relationships
       • Question answering
       • Semantic search
    
    5. NETWORK ANALYSIS:
       • Infrastructure monitoring
       • Vulnerability analysis
       • Optimization
    
    6. BIOLOGY/MEDICINE:
       • Protein interactions
       • Disease spread
       • Drug discovery
    """
    print("\n" + "="*70)
    print("EXAMPLE 5: Real-World Use Cases")
    print("="*70)
    
    print("\n" + "="*70)
    print("USE CASE 1: Social Network - Friend Recommendations")
    print("="*70)
    
    print("\n📊 Creating Social Network...")
    
    users = spark.createDataFrame([
        (1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 28),
        (4, "Diana", 32), (5, "Eve", 27), (6, "Frank", 29)
    ], ["id", "name", "age"])
    
    friendships = spark.createDataFrame([
        (1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (5, 6)
    ], ["src", "dst"])
    
    print("\n   Algorithm: Friends of Friends")
    print("   Recommend: If A→B→C but A not connected to C")
    
    # Find friends of friends
    fof = friendships.alias("f1") \
        .join(friendships.alias("f2"), col("f1.dst") == col("f2.src")) \
        .select(
            col("f1.src").alias("user_id"),
            col("f2.dst").alias("potential_friend")
        ) \
        .join(users.alias("u1"), col("user_id") == col("u1.id")) \
        .join(users.alias("u2"), col("potential_friend") == col("u2.id"))
    
    # Exclude existing friends
    existing = friendships.select(
        col("src").alias("user_id"),
        col("dst").alias("friend_id")
    )
    
    recommendations = fof \
        .join(existing, (col("user_id") == col("existing.user_id")) & \
              (col("potential_friend") == col("friend_id")), "left_anti") \
        .filter(col("user_id") != col("potential_friend")) \
        .select(
            col("u1.name").alias("user"),
            col("u2.name").alias("recommended_friend")
        ) \
        .distinct()
    
    print("\n   Friend Recommendations:")
    recommendations.show()
    
    print("\n" + "="*70)
    print("USE CASE 2: E-commerce - Product Recommendations")
    print("="*70)
    
    print("\n📊 Creating Product Purchase Network...")
    
    products = spark.createDataFrame([
        (101, "Laptop"), (102, "Mouse"), (103, "Keyboard"),
        (104, "Monitor"), (105, "Headphones")
    ], ["id", "name"])
    
    # Edges: Products bought together
    co_purchases = spark.createDataFrame([
        (101, 102, 50),  # 50 customers bought laptop + mouse
        (101, 103, 45),  # 45 customers bought laptop + keyboard
        (101, 104, 30),
        (102, 103, 60),  # Mouse and keyboard often together
        (103, 105, 25)
    ], ["product_1", "product_2", "co_purchase_count"])
    
    print("\n   CO-PURCHASES:")
    co_purchases.show()
    
    print("\n   Algorithm: Frequently Bought Together")
    print("   If user buys Laptop, recommend Mouse (bought together 50 times)")
    
    # Top recommendations for laptop buyers
    laptop_recommendations = co_purchases \
        .filter(col("product_1") == 101) \
        .join(products, col("product_2") == col("id")) \
        .select("name", "co_purchase_count") \
        .orderBy(desc("co_purchase_count"))
    
    print("\n   Recommendations for Laptop buyers:")
    laptop_recommendations.show()
    
    print("\n" + "="*70)
    print("USE CASE 3: Fraud Detection")
    print("="*70)
    
    print("\n📊 Creating Transaction Network...")
    
    accounts = spark.createDataFrame([
        (1, "Account_A"), (2, "Account_B"), (3, "Account_C"),
        (4, "Account_D"), (5, "Account_E")
    ], ["id", "name"])
    
    transactions = spark.createDataFrame([
        (1, 2, 1000), (2, 3, 1000), (3, 1, 1000),  # Circular!
        (4, 5, 500)
    ], ["from_account", "to_account", "amount"])
    
    print("\n   TRANSACTIONS:")
    transactions.show()
    
    print("\n   Fraud Pattern: Circular Transactions")
    print("   A → B → C → A (money laundering indicator)")
    
    # Find circular patterns (triangles)
    circular = transactions.alias("t1") \
        .join(transactions.alias("t2"),
              col("t1.to_account") == col("t2.from_account")) \
        .join(transactions.alias("t3"),
              (col("t2.to_account") == col("t3.from_account")) &
              (col("t3.to_account") == col("t1.from_account")))
    
    if circular.count() > 0:
        print("\n   ⚠️  ALERT: Circular transaction detected!")
        print("   Accounts involved in suspicious pattern:")
        suspicious = circular.select(
            col("t1.from_account").alias("account")
        ).distinct()
        suspicious.show()
    else:
        print("\n   ✅ No circular patterns detected")
    
    print("\n💡 REAL-WORLD APPLICATIONS:")
    print("""
   1. SOCIAL NETWORKS (Facebook, LinkedIn):
      • Friend/connection recommendations
      • Community detection
      • Influencer ranking (PageRank)
      • Spam/fake account detection
   
   2. E-COMMERCE (Amazon, eBay):
      • Product recommendations
      • Frequently bought together
      • User behavior analysis
      • Supply chain optimization
   
   3. FINANCE (Banks, PayPal):
      • Fraud detection (circular transactions)
      • Risk assessment (connected accounts)
      • Money laundering detection
      • Credit scoring (social graph)
   
   4. TRANSPORTATION (Uber, Google Maps):
      • Route optimization
      • Traffic flow analysis
      • Driver-rider matching
      • ETA prediction
   
   5. TELECOMMUNICATIONS:
      • Network topology optimization
      • Call pattern analysis
      • Churn prediction
      • Service quality monitoring
    """)


def main():
    """
    Run all GraphX and GraphFrames examples.
    """
    print("\n" + "="*70)
    print(" GRAPHX AND GRAPHFRAMES - COMPLETE GUIDE ")
    print("="*70)
    
    print("""
Apache Spark Graph Processing:

GRAPHX (Legacy - Pre-Spark 3.0):
• RDD-based API
• Low-level operations
• Lost DataFrame benefits
• Legacy support only

GRAPHFRAMES (Modern - Spark 3.0+):
• DataFrame-based API
• High-level abstractions
• Full SQL support
• DataFrame optimizations
• Recommended for all new projects

This module demonstrates:
1. Basic graph creation (vertices + edges)
2. Graph algorithms (PageRank, connected components, etc.)
3. Motif finding (pattern matching)
4. SQL queries on graphs
5. Real-world use cases

Note: For production use, install GraphFrames:
  pip install graphframes
  OR
  spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12
    """)
    
    try:
        # Create Spark session
        spark = create_graphframes_spark_session()
        
        # Run all examples
        example_1_basic_graph_creation(spark)
        example_2_graph_algorithms(spark)
        example_3_motif_finding(spark)
        example_4_graph_queries_with_sql(spark)
        example_5_real_world_use_cases(spark)
        
        print("\n" + "="*70)
        print("✅ ALL GRAPHFRAMES EXAMPLES COMPLETED SUCCESSFULLY")
        print("="*70)
        
        print("\n📚 KEY TAKEAWAYS:")
        print("""
   1. GRAPHFRAMES vs GRAPHX:
      • GraphFrames: DataFrame-based (recommended)
      • GraphX: RDD-based (legacy)
      • GraphFrames has SQL support + optimizations
   
   2. GRAPH STRUCTURE:
      • Vertices: DataFrame with "id" column
      • Edges: DataFrame with "src" and "dst" columns
      • Both can have additional attributes
   
   3. KEY ALGORITHMS:
      • PageRank: Measure vertex importance
      • Connected Components: Find communities
      • Triangle Count: Measure clustering
      • Shortest Paths: Find shortest routes
   
   4. MOTIF FINDING:
      • Pattern matching in graphs
      • SQL-like syntax
      • Find complex relationships
   
   5. REAL-WORLD APPLICATIONS:
      • Social networks (friend recommendations)
      • E-commerce (product recommendations)
      • Fraud detection (suspicious patterns)
      • Network analysis (optimization)
      • Knowledge graphs (entity relationships)
   
   6. ADVANTAGES:
      • Use SQL on graphs
      • DataFrame operations work
      • Spark optimizations apply
      • Easy integration with ML pipelines
        """)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
