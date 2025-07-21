#!/bin/bash
set -e

PROJECT_VERSION=$1
IMAGE_NAME=$2
DEFAULT_IMAGE_NAME=docker.io/jaihind213/daily_pipeline_car_crash

if [ "$PROJECT_VERSION" == "" ];then
  echo "PROJECT_VERSION not set as 1st argument. bash buildDocker.sh <version>"
  exit 2
fi

if [ "$IMAGE_NAME" == "" ];then
  IMAGE_NAME=$DEFAULT_IMAGE_NAME
fi


if [ "$PUSH_REPO" == "" ];then
  echo "PUSH_REPO not set. Use 'remote' to push to remote registry, 'tar' to save as tar file, or leave empty to load locally."
fi

if [ "$PUSH_LATEST_TAG" == "" ];then
  PUSH_LATEST_TAG="no"
  echo "PUSH_LATEST_TAG not set. Defaulting to 'no'."
fi

if [ "$DOCKERFILE_VERSION" == "" ];then
  DOCKERFILE_VERSION=0.1
  echo "DOCKERFILE_VERSION not set. Defaulting to '0.1'."
fi
if [ "$PLATFORM" == "" ];then
  PLATFORM=linux/amd64
  echo "PLATFORM not set. Defaulting to 'linux/amd64'."
fi

if [ "$PUSH_REPO" == "remote" ];then
  DOCKER_ARGS="${DOCKER_ARGS} --output=type=registry"
elif [ "$PUSH_REPO" == "tar" ];then
  #https://docs.docker.com/reference/cli/docker/buildx/build/
  DOCKER_ARGS="${DOCKER_ARGS} --output type=tar,dest=gopher.tar"
else
  DOCKER_ARGS="${DOCKER_ARGS} --load" #or --output=type=docker both are same
fi
echo DOCKER_ARGS=${DOCKER_ARGS}

#####################
export IMAGE_VERSION="${PROJECT_VERSION}-$DOCKERFILE_VERSION"
TAGS="-t $IMAGE_NAME:$IMAGE_VERSION"

if [ "$PUSH_LATEST_TAG" == "yes" ];then
  echo "setting latest tag..."
  TAGS = "$TAGS -t $IMAGE_NAME:latest"
fi

echo "building docker image... with version $IMAGE_NAME:$IMAGE_VERSION"
echo "PUSH_REPO FLAG: $PUSH_REPO"
echo "TAGS: $TAGS"
echo "PUSH_LATEST_TAG FLAG: $PUSH_LATEST_TAG"
sleep 5
export DOCKER_BUILDKIT=1
docker buildx create --use
echo "docker buildx build $DOCKER_ARGS --platform $PLATFORM $TAGS ."
docker buildx build $DOCKER_ARGS --platform $PLATFORM $TAGS .

touch /tmp/version
cat /dev/null > /tmp/version
echo $IMAGE_VERSION > /tmp/version
