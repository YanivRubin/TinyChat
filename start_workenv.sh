REPO_DIR=$(dirname "${BASH_SOURCE[0]}" | xargs realpath)

# mirror REPO into docker
# mirror the .pi directory so settings & extensions carry over
# currently removed the "--rm" flag; dockers will persist after closing
docker run \
	-it \
	-v "$REPO_DIR":/workdir \
	-v ~/.pi:/settings/.pi \
	-w /workdir \
	python2-3-with-pi:latest \
	bash
