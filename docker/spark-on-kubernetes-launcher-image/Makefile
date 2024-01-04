# Builds a container image used for pi-fhir jobs
SHELL := bash

####
# Arguments that every image-building Makefile should expect
####
DOCKERFILE ?= Dockerfile
CONTEXT ?= ./
DOCKER_REPO_URL ?= localhost:5000/
IMAGE_NAME ?= $(notdir $(CURDIR))
IMAGE_TAG ?= $(USER)-dev
_CLEANED_DOCKER_REPO_URL = $(DOCKER_REPO_URL:/=)
IMAGE_REF ?= $(_CLEANED_DOCKER_REPO_URL)/$(IMAGE_NAME):$(IMAGE_TAG)

####
# Arguments specific to the image built by this Makefile
####

####
# Internal variables
####
all: build

####
# Building the image
####
build:
	DOCKER_BUILDKIT=1 docker build \
		--file ${DOCKERFILE} \
		--tag ${IMAGE_REF} \
		 ${CONTEXT}


####
# Pushing the image
####
push:
	docker push ${IMAGE_REF}
