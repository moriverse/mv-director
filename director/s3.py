import base64
import boto3
import hashlib
import mimetypes
import structlog
import time

from botocore.config import Config
from pydantic import BaseModel
from typing import Any, Callable, Optional

log = structlog.get_logger(__name__)


class UploadParams(BaseModel):
    url: str
    bucket: str
    access_key: str
    secret_key: str
    url_prefix: str
    path_prefix: Optional[str] = None
    object_key: Optional[str] = None


def upload_caller(params: UploadParams) -> Callable[[Any], Optional[str]]:

    config = Config(
        signature_version="s3v4",
        s3={
            "addressing_style": "virtual",
        },
        retries={
            "max_attempts": 10,
            "mode": "standard",
        },
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=params.url,
        aws_access_key_id=params.access_key,
        aws_secret_access_key=params.secret_key,
        config=config,
    )

    def caller(response: Any) -> Any:
        def upload(base64_url: str) -> str:
            try:
                # Extract the content type from the base64 URL
                content_type = base64_url.split(";")[0].split(":")[1]

                # Strip the prefix to get the base64-encoded string
                base64_image = base64_url.split(",")[1]

                # Decode the base64 string to bytes
                image_data = base64.b64decode(base64_image)

                object_key = params.object_key
                if not object_key:
                    # Compute the md5 hash from image_data as object key.
                    object_key = hashlib.md5(image_data).hexdigest()

                    # Add extension if possible.
                    ext = mimetypes.guess_extension(content_type)
                    if ext:
                        object_key = f"{object_key}{ext}"

                    # Add prefix if needed.
                    if params.path_prefix:
                        object_key = f"{params.path_prefix}/{object_key}"

                s3_client.put_object(
                    Bucket=params.bucket,
                    Key=object_key,
                    Body=image_data,
                    ContentType=content_type,
                )

                return f"{params.url_prefix}/{object_key}"

            except Exception:
                log.error(f"Cannot upload file to {params.url}", exc_info=True)
                return base64_url

        log.info("Uploading results.")

        start_time = time.time()

        if isinstance(response, list):
            result = []
            for base64_url in response:
                result.append(upload(base64_url))

        elif isinstance(response, dict):
            result = {}
            for key in response.keys():
                urls = []
                for base64_url in response.get(key, []):
                    urls.append(upload(base64_url))

                result[key] = urls

        else:
            result = response

        elapsed_time = time.time() - start_time
        log.info(f"Results uploaded in {elapsed_time:.2f} seconds")

        return result, elapsed_time

    return caller
