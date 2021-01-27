name := "staging_project_meetup"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"
