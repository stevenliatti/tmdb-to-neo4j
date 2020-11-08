include .env
export

run:
	sbt run

show_env: .env
	@cat .env

run_jar: target/scala-2.12/parser-assembly-0.0.1.jar
	java -Xmx$(JVM_RAM) -jar $<

target/scala-2.12/parser-assembly-0.0.1.jar: src/* project/build.properties project/plugins.sbt build.sbt
	sbt compile && sbt assembly

clean:
	rm -rf target project/target project/project/target
