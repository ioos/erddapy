release:
	rm dist/*
	python setup.py sdist bdist_wheel
	twine upload dist/*

format:
	black --line-length 79 .

test:
	pytest -n 2 -rxs --cov=erddapy tests

lint:
	pytest --flake8 -m flake8

docs:
	pushd docs && make clean html linkcheck && popd

isort:
	isort --remove-import . --apply --use-parentheses --trailing-comma

check: test lint docs isort format
	echo "All checks complete!"
