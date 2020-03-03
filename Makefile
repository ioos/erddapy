release:
	rm -rf dist/
	python setup.py sdist
	pip wheel . -w dist --no-deps

docs:
	cp notebooks/{quick_intro.ipynb,searchfor.ipynb} docs/source/
	pushd docs && make clean html linkcheck && popd

test:
	pytest -n 2 -rxs --cov=erddapy tests

check: style docs lint test
	echo "All checks complete!"
