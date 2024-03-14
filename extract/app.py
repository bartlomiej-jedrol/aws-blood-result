import os
import boto3
import json
import requests
import datetime
from urllib.parse import unquote_plus
from blood_test import morphology
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Return blood result from the file uploaded to the S3 bucket"""
    # Return details of the file uploded to the S3 bucket
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    version = get_version(bucket=bucket, key=key)

    # Extract blood result data from the Textract service
    document = prepare_document(bucket=bucket, key=key, version=version)
    extracted_text = extract_text(document=document)
    blood_result = parse_extracted_text(extracted_text=extracted_text)

    # Insert extracted data to the Airtable
    inserted_payload = insert_to_airtable(blood_result=blood_result)

    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "insertedRows": f"{inserted_payload}",
            }
        ),
    }

    logger.info(f"Response: {response}")


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
    print(document)
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

        # Find the following block to get value for the test
        next_block_index = index + 1
        next_block = extracted_text["Blocks"][next_block_index]["Text"]

        # Trim invalied characters of the next_block
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

        for element in ["^", "/", "%", "fl", "pg"]:
            if element in next_block:
                next_block = next_block[0 : next_block.find(" ")]
                break

        blood_result[test] = next_block
    return blood_result


def insert_to_airtable(blood_result):
    """Insert blood result into the Airtable table"""
    access_token = os.getenv("AIRTABLE_ACCESS_KEY")
    base_id = os.getenv("BASE_ID")
    table_id = os.getenv("TABLE_ID")
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Format values of blood result for the airtable insert
    for k, v in blood_result.items():
        v = v.replace(",", ".")
        blood_result[k] = float(v)

    # Prepare payload for the airtable insert
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime("%Y-%m-%d")
    fields = {"date": formatted_date}
    fields.update(blood_result)
    payload = {"records": [{"fields": fields}]}

    try:
        response = requests.post(url=url, headers=headers, json=payload)
        logger.info(
            f"Successfully inserted payload: {payload} into the Airtable table."
        )
    except Exception as e:
        print(e)
        print(f"Error inserting payload {payload} into the Airtable table.")
        raise e

    return payload
