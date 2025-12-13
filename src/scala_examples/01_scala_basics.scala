/*
 * SCALA BASICS - Understanding the Language
 * 
 * WHAT IS SCALA?
 * ==============
 * Scala = "Scalable Language"
 * - Runs on Java Virtual Machine (JVM)
 * - Combines Object-Oriented + Functional Programming
 * - Statically typed with type inference
 * - Compiles to Java bytecode
 * - Created by Martin Odersky (2003)
 * - Used by: Twitter, LinkedIn, Netflix, Apache Spark
 * 
 * SIMILAR TO:
 * ===========
 * 1. Java - Same JVM, can use Java libraries
 * 2. Python - Concise syntax, functional features
 * 3. Kotlin - Modern JVM language
 * 4. Rust - Strong type system, pattern matching
 * 5. Haskell - Functional programming concepts
 * 
 * KEY DIFFERENCES FROM PYTHON:
 * ============================
 * Scala                     | Python
 * --------------------------|---------------------------
 * Statically typed          | Dynamically typed
 * Compiled                  | Interpreted
 * val (immutable default)   | Variables mutable by default
 * Pattern matching          | if/elif/else chains
 * Case classes              | dataclasses (Python 3.7+)
 * Traits                    | Abstract classes/interfaces
 * Type inference            | Duck typing
 */

// =============================================================================
// 1. VARIABLES AND VALUES
// =============================================================================

object ScalaBasics {
  
  def main(args: Array[String]): Unit = {
    
    // VAL = IMMUTABLE (like Python's constant, but enforced)
    // Cannot be reassigned after initialization
    val name: String = "Alice"       // Explicit type
    val age = 30                     // Type inference (Int)
    val pi = 3.14159                 // Type inference (Double)
    
    // name = "Bob"  // ERROR: Cannot reassign val
    
    // VAR = MUTABLE (like Python's regular variable)
    var counter = 0
    counter = 1                      // OK: Can reassign var
    counter += 10                    // OK
    
    println(s"Name: $name, Age: $age")  // String interpolation
    println(s"Counter: $counter")
    
    // COMPARISON WITH PYTHON:
    // Python: name = "Alice"  (mutable by default)
    // Scala: val name = "Alice"  (immutable by default)
  }
}

// =============================================================================
// 2. DATA TYPES
// =============================================================================

object DataTypes {
  
  // Scala has more precise numeric types than Python
  val byteVal: Byte = 127           // 8-bit signed (-128 to 127)
  val shortVal: Short = 32767       // 16-bit signed
  val intVal: Int = 2147483647      // 32-bit signed (default)
  val longVal: Long = 9223372036854775807L  // 64-bit signed
  
  val floatVal: Float = 3.14f       // 32-bit floating point
  val doubleVal: Double = 3.14159   // 64-bit floating point (default)
  
  val boolVal: Boolean = true       // true or false
  val charVal: Char = 'A'           // Single character (16-bit Unicode)
  val stringVal: String = "Hello"   // String (immutable)
  
  // UNIT = Like Python's None (represents absence of value)
  def printHello(): Unit = {
    println("Hello")
    // No return value
  }
  
  // ANY = Like Python's object (root of type hierarchy)
  val anything: Any = 42
  val anything2: Any = "String"
  
  // NOTHING = Like Python's NoReturn (subtype of everything)
  def throwError(): Nothing = {
    throw new Exception("Error!")
  }
}

// =============================================================================
// 3. COLLECTIONS (Like Python's list, dict, set)
// =============================================================================

object Collections {
  
  // LIST = Immutable linked list (like Python's tuple in behavior)
  val numbers: List[Int] = List(1, 2, 3, 4, 5)
  val fruits = List("apple", "banana", "cherry")  // Type inferred
  
  // Access elements (0-indexed like Python)
  val first = numbers.head         // 1 (first element)
  val rest = numbers.tail          // List(2, 3, 4, 5)
  val third = numbers(2)           // 3 (0-indexed)
  
  // List operations (functional style)
  val doubled = numbers.map(_ * 2)           // List(2, 4, 6, 8, 10)
  val evens = numbers.filter(_ % 2 == 0)     // List(2, 4)
  val sum = numbers.reduce(_ + _)            // 15
  
  // ARRAY = Mutable, fixed-size (like Python's list but fixed size)
  val arr: Array[Int] = Array(1, 2, 3, 4, 5)
  arr(0) = 10                     // OK: Arrays are mutable
  
  // MAP = Key-value pairs (like Python's dict)
  val ages: Map[String, Int] = Map(
    "Alice" -> 30,
    "Bob" -> 25,
    "Charlie" -> 35
  )
  
  val aliceAge = ages("Alice")              // 30
  val aliceAge2 = ages.get("Alice")         // Some(30) (Option type)
  val unknownAge = ages.get("Dave")         // None
  
  // SET = Unique elements (like Python's set)
  val uniqueNumbers: Set[Int] = Set(1, 2, 3, 2, 1)  // Set(1, 2, 3)
  
  // COMPARISON WITH PYTHON:
  // Python: numbers = [1, 2, 3]  (mutable list)
  // Scala:  val numbers = List(1, 2, 3)  (immutable list)
  // 
  // Python: ages = {"Alice": 30}
  // Scala:  val ages = Map("Alice" -> 30)
}

// =============================================================================
// 4. FUNCTIONS
// =============================================================================

object Functions {
  
  // Basic function definition
  def add(x: Int, y: Int): Int = {
    x + y  // Last expression is returned (no 'return' needed)
  }
  
  // Single-expression function (concise)
  def multiply(x: Int, y: Int): Int = x * y
  
  // Function with default parameters
  def greet(name: String, greeting: String = "Hello"): String = {
    s"$greeting, $name!"
  }
  
  // Function with multiple parameter lists (currying)
  def addThenMultiply(x: Int)(y: Int)(z: Int): Int = {
    (x + y) * z
  }
  
  // Higher-order function (takes function as parameter)
  def applyTwice(f: Int => Int, x: Int): Int = {
    f(f(x))
  }
  
  // Anonymous function (lambda in Python)
  val square = (x: Int) => x * x
  val increment = (x: Int) => x + 1
  
  // Using higher-order functions
  val result1 = applyTwice(square, 3)        // square(square(3)) = 81
  val result2 = applyTwice(increment, 5)     // increment(increment(5)) = 7
  
  // COMPARISON WITH PYTHON:
  // Python: def add(x, y): return x + y
  // Scala:  def add(x: Int, y: Int): Int = x + y
  // 
  // Python: square = lambda x: x * x
  // Scala:  val square = (x: Int) => x * x
}

// =============================================================================
// 5. CASE CLASSES (Like Python's @dataclass)
// =============================================================================

// Case classes are immutable data containers
// Similar to Python's dataclasses (Python 3.7+)
case class Person(name: String, age: Int, city: String)

object CaseClassesExample {
  
  val alice = Person("Alice", 30, "NYC")
  val bob = Person("Bob", 25, "SF")
  
  // Automatic toString
  println(alice)  // Person(Alice,30,NYC)
  
  // Automatic equals and hashCode
  val alice2 = Person("Alice", 30, "NYC")
  val areEqual = alice == alice2  // true (compares values)
  
  // Copy method (create modified copy)
  val olderAlice = alice.copy(age = 31)
  
  // Pattern matching (see below)
  
  // COMPARISON WITH PYTHON:
  // Python (3.7+):
  // @dataclass
  // class Person:
  //     name: str
  //     age: int
  //     city: str
  // 
  // Scala:
  // case class Person(name: String, age: Int, city: String)
}

// =============================================================================
// 6. PATTERN MATCHING (Like Python's match statement, but more powerful)
// =============================================================================

object PatternMatching {
  
  // Basic pattern matching
  def describe(x: Any): String = x match {
    case 0 => "zero"
    case 1 => "one"
    case n: Int if n > 1 => s"positive integer: $n"
    case n: Int if n < 0 => s"negative integer: $n"
    case s: String => s"string: $s"
    case _ => "something else"  // Default case (like _ in Python)
  }
  
  // Pattern matching with case classes
  case class Person(name: String, age: Int)
  
  def greetPerson(person: Person): String = person match {
    case Person("Alice", _) => "Hi Alice!"
    case Person(name, age) if age < 18 => s"Hello young $name"
    case Person(name, age) if age >= 65 => s"Good day, $name"
    case Person(name, _) => s"Hello $name"
  }
  
  // Pattern matching with lists
  def sumFirstTwo(list: List[Int]): Int = list match {
    case Nil => 0                    // Empty list
    case x :: Nil => x               // Single element
    case x :: y :: _ => x + y        // Two or more elements
  }
  
  // COMPARISON WITH PYTHON (3.10+):
  // Python:
  // match x:
  //     case 0:
  //         return "zero"
  //     case _:
  //         return "other"
  // 
  // Scala:
  // x match {
  //   case 0 => "zero"
  //   case _ => "other"
  // }
}

// =============================================================================
// 7. OPTION TYPE (Like Python's Optional)
// =============================================================================

object OptionExample {
  
  // Option[T] represents optional values (may or may not exist)
  // Replaces null pointer errors
  
  def divide(x: Int, y: Int): Option[Double] = {
    if (y != 0) Some(x.toDouble / y)
    else None
  }
  
  val result1 = divide(10, 2)  // Some(5.0)
  val result2 = divide(10, 0)  // None
  
  // Using pattern matching with Option
  result1 match {
    case Some(value) => println(s"Result: $value")
    case None => println("Cannot divide by zero")
  }
  
  // Using map, flatMap, filter (functional style)
  val doubled = result1.map(_ * 2)          // Some(10.0)
  val default = result2.getOrElse(0.0)      // 0.0
  
  // COMPARISON WITH PYTHON:
  // Python (3.5+):
  // from typing import Optional
  // def divide(x: int, y: int) -> Optional[float]:
  //     if y != 0:
  //         return x / y
  //     return None
  // 
  // Scala:
  // def divide(x: Int, y: Int): Option[Double] = {
  //   if (y != 0) Some(x.toDouble / y)
  //   else None
  // }
}

// =============================================================================
// 8. TRAITS (Like Python's Abstract Classes)
// =============================================================================

// Trait = Interface + default implementations
trait Animal {
  def name: String              // Abstract (must be implemented)
  def sound: String             // Abstract
  
  def makeSound(): Unit = {     // Concrete (has implementation)
    println(s"$name says $sound")
  }
}

class Dog(val name: String) extends Animal {
  def sound: String = "Woof!"
}

class Cat(val name: String) extends Animal {
  def sound: String = "Meow!"
}

object TraitsExample {
  val dog = new Dog("Buddy")
  val cat = new Cat("Whiskers")
  
  dog.makeSound()  // Buddy says Woof!
  cat.makeSound()  // Whiskers says Meow!
  
  // COMPARISON WITH PYTHON:
  // Python:
  // from abc import ABC, abstractmethod
  // class Animal(ABC):
  //     @abstractmethod
  //     def sound(self) -> str:
  //         pass
  // 
  // Scala:
  // trait Animal {
  //   def sound: String
  // }
}

// =============================================================================
// 9. FOR COMPREHENSIONS (Like Python's list comprehensions)
// =============================================================================

object ForComprehensions {
  
  val numbers = List(1, 2, 3, 4, 5)
  
  // Simple for loop
  for (n <- numbers) {
    println(n)
  }
  
  // For with yield (creates new collection)
  val doubled = for (n <- numbers) yield n * 2  // List(2, 4, 6, 8, 10)
  
  // For with filter
  val evens = for (n <- numbers if n % 2 == 0) yield n  // List(2, 4)
  
  // Nested for (like nested loops)
  val pairs = for {
    x <- List(1, 2, 3)
    y <- List("a", "b")
  } yield (x, y)
  // Result: List((1,a), (1,b), (2,a), (2,b), (3,a), (3,b))
  
  // COMPARISON WITH PYTHON:
  // Python: [n * 2 for n in numbers]
  // Scala:  for (n <- numbers) yield n * 2
  // 
  // Python: [n for n in numbers if n % 2 == 0]
  // Scala:  for (n <- numbers if n % 2 == 0) yield n
}

// =============================================================================
// 10. IMPLICIT CONVERSIONS (Scala-specific feature)
// =============================================================================

object ImplicitsExample {
  
  // Implicit conversion (automatic type conversion)
  implicit def intToString(x: Int): String = x.toString
  
  // Now Int can be used where String is expected
  def printString(s: String): Unit = println(s)
  
  // This works because of implicit conversion
  // printString(42)  // Automatically converts 42 to "42"
  
  // Implicit parameters (automatically passed)
  implicit val defaultGreeting: String = "Hello"
  
  def greet(name: String)(implicit greeting: String): String = {
    s"$greeting, $name!"
  }
  
  // Automatically uses implicit value
  val result = greet("Alice")  // "Hello, Alice!"
  
  // Can override by providing explicit value
  val result2 = greet("Bob")("Hi")  // "Hi, Bob!"
  
  // NOTE: Implicits are powerful but can make code hard to understand
  // Use sparingly and document well
}

// =============================================================================
// SUMMARY: SCALA KEY CONCEPTS
// =============================================================================

/*
 * SCALA vs PYTHON QUICK REFERENCE:
 * 
 * 1. VARIABLES
 *    Python: x = 10 (mutable)
 *    Scala:  val x = 10 (immutable), var x = 10 (mutable)
 * 
 * 2. FUNCTIONS
 *    Python: def add(x, y): return x + y
 *    Scala:  def add(x: Int, y: Int): Int = x + y
 * 
 * 3. LAMBDAS
 *    Python: lambda x: x * 2
 *    Scala:  (x: Int) => x * 2  or  _ * 2 (shorthand)
 * 
 * 4. LIST COMPREHENSION
 *    Python: [x * 2 for x in numbers]
 *    Scala:  for (x <- numbers) yield x * 2
 * 
 * 5. DATA CLASSES
 *    Python: @dataclass class Person: ...
 *    Scala:  case class Person(...)
 * 
 * 6. OPTIONAL VALUES
 *    Python: Optional[int]
 *    Scala:  Option[Int]
 * 
 * 7. PATTERN MATCHING
 *    Python: match x: case 0: ...
 *    Scala:  x match { case 0 => ... }
 * 
 * WHY SCALA FOR SPARK?
 * ====================
 * 1. Spark is written in Scala (native performance)
 * 2. Type safety catches errors at compile time
 * 3. UDFs are 2-5x faster than Python UDFs
 * 4. Full access to all Spark features
 * 5. Better for low-latency, high-throughput applications
 * 
 * WHEN TO USE SCALA?
 * ==================
 * ✓ Performance-critical applications
 * ✓ Low-latency requirements (<100ms)
 * ✓ Large-scale production systems
 * ✓ Custom Spark development
 * ✓ Team has JVM expertise
 * 
 * WHEN TO USE PYSPARK?
 * ====================
 * ✓ Data science and ML focus
 * ✓ Rapid prototyping
 * ✓ Python ecosystem integration
 * ✓ Easier learning curve
 * ✓ Team has Python expertise
 */
