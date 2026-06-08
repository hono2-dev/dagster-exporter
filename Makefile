IMAGE_REPO ?= 677929148633.dkr.ecr.ap-northeast-1.amazonaws.com/dagster/dagster-exporter
TAG        ?= latest
PLATFORM   ?= linux/amd64
REGION     ?= ap-northeast-1

.PHONY: build push release tag-latest ecr-login

build:
	docker build \
		--platform $(PLATFORM) \
		--provenance=false \
		-t $(IMAGE_REPO):$(TAG) \
		.

push:
	docker push $(IMAGE_REPO):$(TAG)

release: build push

tag-latest:
	docker tag $(IMAGE_REPO):$(TAG) $(IMAGE_REPO):latest
	docker push $(IMAGE_REPO):latest

ecr-login:
	aws ecr get-login-password --region $(REGION) | \
		docker login --username AWS --password-stdin \
		677929148633.dkr.ecr.ap-northeast-1.amazonaws.com
