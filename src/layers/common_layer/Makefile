build-CommonLayer:
	mkdir -p "$(ARTIFACTS_DIR)/python"

	python -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python" \
		--platform manylinux2014_aarch64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		--upgrade

	cp -r common "$(ARTIFACTS_DIR)/python/"
	rm -rf "$(ARTIFACTS_DIR)/python/*.dist-info"
	find "$(ARTIFACTS_DIR)/python" -type d -name "__pycache__" -exec rm -rf {} +