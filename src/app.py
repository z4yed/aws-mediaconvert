import os
import json
import logging
import uuid
import random
from urllib.parse import urlparse

import boto3

# Define the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

def override_job_encryption(job_settings, static_key, key_provider_url):
    # Check if the job_settings contain HLS encryption settings
    if 'OutputGroups' in job_settings:
        for output_group in job_settings['OutputGroups']:
            if 'HlsGroupSettings' in output_group.get('OutputGroupSettings', {}):
                encryption_settings = output_group['OutputGroupSettings']['HlsGroupSettings'].get('Encryption', {})
                static_key_provider = encryption_settings.get('StaticKeyProvider', {})
                # Override StaticKey value if it exists
                if 'StaticKeyValue' in static_key_provider:
                    static_key_provider['StaticKeyValue'] = static_key
                # Override Url if it exists
                if 'Url' in static_key_provider:
                    static_key_provider['Url'] = key_provider_url

def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    source_s3 = f's3://{source_bucket}/{source_key}'

    destination_bucket = source_bucket  # Use the same bucket for output
    media_convert_role = os.environ['MediaConvertRole']
    application = os.environ['Application']
    region = os.environ['AWS_DEFAULT_REGION']
    status_code = 200
    jobs = []
    job_metadata = {
        'assetID': str(uuid.uuid4()),  # Generating a unique asset ID
        'application': application,
        'input': source_s3
    }

    job = None  # Initialize job variable

    try:
        bucket = s3.Bucket(source_bucket)

        # Find job settings files in the jobs/ folder
        for obj in bucket.objects.filter(Prefix='jobs/'):
            if obj.key != "jobs/":
                job_input = {
                    'filename': obj.key,
                    'settings': json.loads(obj.get()['Body'].read())
                }
                jobs.append(job_input)

        # Use Default job settings if no job settings files are found
        if not jobs:
            with open('job.json') as json_data:
                job_input = {
                    'filename': 'Default',
                    'settings': json.load(json_data)
                }
                jobs.append(job_input)

        mediaconvert_client = boto3.client('mediaconvert', region_name=region)
        endpoints = mediaconvert_client.describe_endpoints()
        client = boto3.client('mediaconvert', region_name=region, endpoint_url=endpoints['Endpoints'][0]['Url'],
                              verify=False)

        # Get all the StaticKey environment variables
        static_keys = [os.environ[key] for key in os.environ if key.startswith('StaticKey')]
        # Randomly select one of the StaticKey values
        static_key = random.choice(static_keys)
        key_provider_url = os.environ['KeyProviderUrl']

        for job_input in jobs:
            job_settings = job_input['settings']
            job_filename = job_input['filename']

            job_metadata['settings'] = job_filename
            job_settings['Inputs'][0]['FileInput'] = source_s3

            # Construct the output directory path in the output bucket
            output_directory = os.path.dirname(source_key)
            output_directory = output_directory.lstrip('/')
            output_directory = output_directory.replace('inputs', 'outputs')

            for output_group in job_settings['OutputGroups']:
                if output_group['OutputGroupSettings']['Type'] == 'HLS_GROUP_SETTINGS':
                    template_destination = output_group['OutputGroupSettings']['HlsGroupSettings']['Destination']
                    template_destination_key = urlparse(template_destination).path
                    output_group['OutputGroupSettings']['HlsGroupSettings']['Destination'] = f's3://{destination_bucket}/{output_directory}{template_destination_key}'

                if output_group['OutputGroupSettings']['Type'] == 'FILE_GROUP_SETTINGS':
                    template_destination = output_group['OutputGroupSettings']['FileGroupSettings']['Destination']
                    template_destination_key = urlparse(template_destination).path
                    output_group['OutputGroupSettings']['FileGroupSettings']['Destination'] = f's3://{destination_bucket}/{output_directory}{template_destination_key}'

            # Override StaticKey value if provided
            if static_key:
                override_job_encryption(job_settings, static_key, key_provider_url)
                job_metadata['staticKey'] = static_key

            print('Static key used: ', static_key)
            print('Job setting: ', job_settings)
            print('metadata: ', job_metadata)

            job = client.create_job(Role=media_convert_role, UserMetadata=job_metadata, Settings=job_settings)

    except Exception as e:
        logger.error('Exception: %s', e)
        status_code = 500

    return {
        'statusCode': status_code,
        'body': json.dumps(job, indent=4, sort_keys=True, default=str),
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
    }
