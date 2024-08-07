import base64
import boto3
import hashlib
import structlog

from botocore.config import Config
from pydantic import BaseModel
from typing import Any, Callable, List, Optional

log = structlog.get_logger(__name__)


class UploadParams(BaseModel):
    url: str
    bucket: str
    access_key: str
    secret_key: str
    url_prefix: str
    path_prefix: Optional[str] = None


def upload_caller(params: UploadParams) -> Callable[[Any], Optional[str]]:

    retry_config = Config(
        retries={
            "max_attempts": 10,
            "mode": "standard",
        }
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=params.url,
        aws_access_key_id=params.access_key,
        aws_secret_access_key=params.secret_key,
        config=retry_config,
    )

    def caller(response: List[str]) -> Optional[str]:
        def upload(base64_url: str):
            try:
                # Extract the content type from the base64 URL
                content_type = base64_url.split(";")[0].split(":")[1]

                # Strip the prefix to get the base64-encoded string
                base64_image = base64_url.split(",")[1]

                # Decode the base64 string to bytes
                image_data = base64.b64decode(base64_image)

                # Compute the md5 hash from image_data as object key.
                object_key = hashlib.md5(image_data).hexdigest()
                if params.path_prefix is not None:
                    object_key = f"{params.path_prefix}/{object_key}"

                s3_client.put_object(
                    Bucket=params.bucket,
                    Key=object_key,
                    Body=image_data,
                    ContentType=content_type,
                )

                return f"{params.url}/{params.bucket}/{object_key}"

            except Exception as e:
                log.error(f"Cannot upload file to {params.url}, error: {e}")
                return None

        url_list = []
        for url in response:
            uploaded = upload(url)
            if uploaded:
                url_list.append(uploaded)

        return url_list

    return caller
