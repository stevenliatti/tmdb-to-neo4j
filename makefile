include .env
export

run:
	sbt run

show_env: .env
	@cat .env

run_jar: target/scala-2.13/parser-assembly-0.0.1.jar
	java -Xmx$(JVM_RAM) -jar $<

target/scala-2.13/parser-assembly-0.0.1.jar: src/* project/build.properties project/plugins.sbt build.sbt
	sbt compile && sbt assembly

data/actors.json: data/actors.json.xz
	unxz --keep $^

data/movies.json: data/movies.json.xz
	unxz --keep $^

data/actors.json.xz: data
	wget -O $@ https://drive.switch.ch/index.php/s/aMZeySmczIYpk55/download

data/movies.json.xz: data
	wget -O $@ https://drive.switch.ch/index.php/s/CNXvGp4fvW4XLXo/download

data:
	mkdir -p data

clean:
	rm -rf target project/target project/project/target

neo4j: neo4j/plugins/neo4j-graph-data-science-1.4.0-standalone.jar
	docker run --rm \
	--publish=7474:7474 --publish=7687:7687 \
	--volume="$$PWD"/neo4j/conf:/conf \
	--volume="$$PWD"/neo4j/plugins:/plugins \
	--env-file=.env \
	--user="$$(id -u):$$(id -g)" \
	neo4j

neo4j_vol: neo4j/plugins/neo4j-graph-data-science-1.4.0-standalone.jar
	docker run --rm -d \
	--publish=7474:7474 --publish=7687:7687 \
	--volume="$$PWD"/neo4j/conf:/conf \
	--volume="$$PWD"/neo4j/plugins:/plugins \
	--volume="$$PWD"/neo4j/data:/data \
	--env-file=.env \
	--user="$$(id -u):$$(id -g)" \
	neo4j

neo4j/plugins/neo4j-graph-data-science-1.4.0-standalone.jar:
	mkdir -p neo4j/plugins neo4j/conf neo4j/data
	-wget -nc https://s3-eu-west-1.amazonaws.com/com.neo4j.graphalgorithms.dist/graph-data-science/neo4j-graph-data-science-1.4.0-standalone.zip --directory-prefix=neo4j/plugins
	unzip neo4j/plugins/neo4j-graph-data-science-1.4.0-standalone.zip -d neo4j/plugins
	rm neo4j/plugins/neo4j-graph-data-science-1.4.0-standalone.zip

.PHONY: neo4j
