import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences.{ AlignParameters, AlignSingleLineCaseStatements }

name := "finagle-http4s"

organization := "org.bitbucket.lleggieri"

scalaVersion := "2.11.8"

val FINAGLE_VERSION = "6.35.0"

val HTTP4S_VERSION = "0.13.2"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignParameters, true)

libraryDependencies ++= Seq(
  "com.twitter"     %%   "finagle-core"          % FINAGLE_VERSION   % "provided",
  "com.twitter"     %%   "finagle-http"          % FINAGLE_VERSION   % "provided",
  "com.twitter"     %%   "finagle-netty4-http"   % FINAGLE_VERSION   % "provided",
  "com.twitter"     %%   "finagle-netty4"        % FINAGLE_VERSION   % "provided",
  "org.http4s"      %%   "http4s-core"           % HTTP4S_VERSION    % "provided",
  "org.http4s"      %%   "http4s-server"         % HTTP4S_VERSION    % "provided",
  "org.http4s"      %%   "http4s-client"         % HTTP4S_VERSION    % "provided",
  "org.scodec"      %%   "scodec-scalaz"         % "1.0.0"           % "provided",
  "org.slf4j"        %   "jul-to-slf4j"          % "1.7.12"
)
    