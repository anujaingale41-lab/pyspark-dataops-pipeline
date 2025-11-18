.PHONY: run test install format

install:
	pip install -r requirements.txt

run:
	python run_job.py

test:
	pytest -v

format:
	black .
