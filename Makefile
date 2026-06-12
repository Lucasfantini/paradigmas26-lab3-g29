compile:
	sbt compile

run:
	sbt "run --subscription-file data/valid_subscriptions.json --entities-dir data/valid_entities --top-k 10"

test:
	bash tests.sh

clean:
	sbt clean
