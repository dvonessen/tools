import boto3
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s'
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('--profile-name', '-p', default='default', help='AWS profile name configure in aws config files.')
parser.add_argument('--source-bucket', '-s', help='Source Bucket to sync diff from.', required=True)
parser.add_argument('--destination-bucket', '-d', help='Destination Bucket to sync to.', required=True)
parser.add_argument('--prefix', default='', help='Prefix to list objects from and to sync to.')
parser.add_argument('--verbose', '-v', action='count')
cmd_args = parser.parse_args()

if cmd_args.verbose and cmd_args.verbose > 0:
    logger.setLevel(logging.DEBUG)

try:
    session = boto3.session.Session(profile_name=cmd_args.profile_name)
    s3_client = session.client('s3')
    dest_bucket_resource = session.resource('s3')
except:
    logger.debug('', exc_info=True)
    sys.exit(127)
else:

    source_bucket_resp = s3_client.list_objects_v2(
            Bucket=cmd_args.source_bucket,
            Prefix=cmd_args.prefix
        )

    source_bucket_keys = list()
    while True:
        for bucket_key in source_bucket_resp['Contents']:
            source_bucket_keys.append(bucket_key['Key'])

        if source_bucket_resp['IsTruncated']:
            source_bucket_resp = s3_client.list_objects_v2(
                Bucket=cmd_args.source_bucket,
                Prefix=cmd_args.prefix,
                ContinuationToken=source_bucket_resp['NextContinuationToken']
            )
        else:
            source_bucket_resp = s3_client.list_objects_v2(
                Bucket=cmd_args.source_bucket,
                Prefix=cmd_args.prefix
            )
            break
    logger.debug('Count of Keys in source bucket: {}'.format(len(source_bucket_keys)))

    dest_bucket_resp = s3_client.list_objects_v2(
            Bucket=cmd_args.destination_bucket,
            Prefix=cmd_args.prefix
        )

    dest_bucket_keys = list()
    while True:
        for bucket_key in dest_bucket_resp['Contents']:
            dest_bucket_keys.append(bucket_key['Key'])

        if dest_bucket_resp['IsTruncated']:
            dest_bucket_resp = s3_client.list_objects_v2(
                Bucket=cmd_args.destination_bucket,
                Prefix=cmd_args.prefix,
                ContinuationToken=dest_bucket_resp['NextContinuationToken']
            )
        else:
            dest_bucket_resp = s3_client.list_objects_v2(
                Bucket=cmd_args.destination_bucket,
                Prefix=cmd_args.prefix
            )
            break
    logger.debug('Count of Keys in destination bucket: {}'.format(len(dest_bucket_keys)))

    diff_key = set(source_bucket_keys) - set(dest_bucket_keys)
    logger.debug('Count of Objects not in destination Bucket: {}'.format(len(diff_key)))
    logger.debug('Objects not in destination Bucket: {}'.format(diff_key))

    for key in diff_key:
        resource = session.resource('s3')
        dest_bucket = resource.Bucket(cmd_args.destination_bucket)
        dest_obj = dest_bucket.Object(key)
        logger.debug('Copying key: {}'.format(key))
        dest_obj.copy(
            {
                'Bucket': cmd_args.source_bucket,
                'Key': key
            }
        )
