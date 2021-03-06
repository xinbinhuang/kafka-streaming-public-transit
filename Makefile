.PHONY: init simulation ksql faust server

init:
	pipenv install

simulation:
	pipenv run python producers/simulation.py

ksql:
	pipenv run python consumers/ksql.py 

faust:
	cd consumers; pipenv run faust -A faust_stream worker -l info

server:
	pipenv run python consumers/server.py
	