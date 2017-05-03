#! /bin/bash

# exit on the first error
set -e

if [ "$1" == "" ]; then
    echo Usage: $0 pilosa_build_image_name
    exit 1
fi

IMAGE_NAME=$1-micro
echo Building image: ${IMAGE_NAME}

# fetch pilosa executable from the build container
CONTAINER=$(docker run -d $1)
docker cp ${CONTAINER}:/pilosa .
docker stop ${CONTAINER}
docker rm ${CONTAINER}

# build the slim image
docker build -t ${IMAGE_NAME} .

# cleanup
rm pilosa
