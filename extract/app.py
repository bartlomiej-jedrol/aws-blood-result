import boto3
import json
from urllib.parse import unquote_plus
from blood_test import morphology
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Return blood result from the file uploaded to the S3 bucket"""
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    version = get_version(bucket=bucket, key=key)
    document = prepare_document(bucket=bucket, key=key, version=version)
    extracted_text = extract_text(document=document)
    blood_result = parse_extracted_text(extracted_text=extracted_text)

    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "bloodResult": f"{blood_result}",
            }
        ),
    }

    logger.info(response)
    return response


def get_version(bucket, key):
    """Return version of the file"""
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object_attributes(
            Bucket=bucket,
            Key=key,
            ObjectAttributes=["ETag"],
        )
        logger.info(
            f"Successfully obtained object {key} details from the bucket {bucket}. Response: {response}"
        )
        return response["VersionId"]
    except Exception as e:
        print(e)
        print(
            f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function."
        )
        raise e


def prepare_document(bucket, key, version):
    """Prepare and return document dict for Textract"""
    return {
        "S3Object": {
            "Bucket": bucket,
            "Name": key,
            "Version": version,
        }
    }


def extract_text(document):
    """Extract and return text from document using the Textract service"""
    textract_client = boto3.client("textract")
    try:
        response = textract_client.analyze_document(
            Document=document, FeatureTypes=["TABLES"]
        )
        logger.info(f"Successfully extracted text from the Textract service.")
        return response
    except Exception as e:
        print(e)
        print(f"Error getting extracted text for the document {document}.")
        raise e


def parse_extracted_text(extracted_text):
    """Parse and return extracted blood result"""
    blood_result = {}
    for test in morphology:
        # Find index of a block containing result for a test in morphology
        index = next(
            (
                index
                for (index, d) in enumerate(extracted_text["Blocks"])
                if d.get("Text") and test in d.get("Text")
            ),
            None,
        )
        # Find following block to get value for the test
        next_block_index = index + 1
        next_block = extracted_text["Blocks"][next_block_index]["Text"]
        # Trim value to remove not required characters
        if (
            test
            in (
                "BASO",
                "NEU%",
                "LYMPH%",
                "MON%",
                "EOS%",
            )
            and next_block.find(" ") != -1  # Prevent from trimming an actual value
        ):
            next_block = next_block[0 : next_block.find(" ")]

        blood_result[test] = next_block
    return blood_result
