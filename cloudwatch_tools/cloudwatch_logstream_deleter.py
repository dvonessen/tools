import boto3
import argparse
import logging
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--profile', help='AWS Profile to use.')
parser.add_argument('-l', '--log-group', help='Specify Loggroup to delete entries')
parser.add_argument('-a', '--role-arn', help='Specify role arn to use.')
parser.add_argument('-v', '--verbose', action='store_true', help='Increases verbosity.')
args = parser.parse_args()

if len(sys.argv) <= 1:
    parser.print_help()
    sys.exit(0)


def get_logger(debug=False):

    log_level = logging.ERROR

    if debug:
        log_level = logging.DEBUG

    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    return logger


def main():

    logger = get_logger(debug=args.verbose)

    try:
        boto_session = boto3.Session(profile_name=args.profile.strip())
        sts = boto_session.client('sts')
        sts_response = sts.assume_role(
            RoleArn=args.role_arn.strip(),
            RoleSessionName='cloudwatch_logs_deleter'
        )
    except:
        logger.error('Unable to locate credentials')
        logger.debug('', exc_info=True)
        sys.exit(127)

    try:
        sts_access_key = sts_response['Credentials']['AccessKeyId']
        sts_secret_key = sts_response['Credentials']['SecretAccessKey']
        sts_sec_token = sts_response['Credentials']['SessionToken']
    except:
        logger.error('AccessKeyId or SecretAccessKey not available!')
        logger.debug('', exc_info=True)
        sys.exit(127)

    try:
        nextToken = None
        cw_logs_client = boto3.client(
            'logs',
            aws_access_key_id=sts_access_key,
            aws_secret_access_key=sts_secret_key,
            aws_session_token=sts_sec_token
            )

        log_stream_names = list()

        while True:
            if nextToken:
                log_streams = cw_logs_client.describe_log_streams(
                    logGroupName=args.log_group.strip(),
                    nextToken=nextToken
                )
            else:
                log_streams = cw_logs_client.describe_log_streams(
                    logGroupName=args.log_group.strip()
                )

            if 'nextToken' in log_streams:
                nextToken = log_streams['nextToken']
            else:
                nextToken = None

            for i in log_streams['logStreams']:
                log_stream_names.append(i['logStreamName'])

            if not nextToken:
                break

        for log_stream_name in log_stream_names:
            resp = cw_logs_client.delete_log_stream(
                logGroupName=args.log_group.strip(),
                logStreamName=log_stream_name
            )

            if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
                print('Deletion successfull LogstreamName: {}'.format(log_stream_name))
            else:
                logger.error(resp)

    except:
        logger.error('Error in deleting logs from loggroup.')
        logger.debug('', exc_info=True)
        sys.exit(127)

if __name__ == '__main__':
    main()
