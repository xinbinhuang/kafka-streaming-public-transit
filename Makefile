iPHONY: init simulation ksql faust server all

init:
	pipenv install

simulation:
	pipenv run python producers/simulation.py

ksql:
	pipenv run python consumers/ksql.py 

faust:
	pipenv run faust -A consumers.faust_stream worker -l info

server:
	pipenv run python consumers/server.py
	