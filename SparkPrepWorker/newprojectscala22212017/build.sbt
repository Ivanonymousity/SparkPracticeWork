version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-sql" % "2.1.0",
	"org.apache.derby" % "derby" % "10.4.1.3",
	"org.apache.spark" %% "spark-core" % "2.1.0"
	)