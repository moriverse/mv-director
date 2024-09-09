ARTIFACT_URL = siwu-cn-shanghai.cr.volces.com/job/director

build:
	docker build --network host -t $(ARTIFACT_URL) . && docker push $(ARTIFACT_URL)