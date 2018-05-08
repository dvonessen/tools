import boto3
import argparse
import threading
import sys
import logging
import queue
import time

q = queue.Queue()

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s"
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    '--bucket',
    '-b',
    help='Bucket to change storage class.',
    required=True
)
parser.add_argument(
    '--storage-class',
    choices=['STANDARD', 'STANDARD_IA', 'ONEZONE_IA', 'REDUCED_REDUNDANCY'],
    help='Storage class you want to transmission your objects.',
    default='STANDARD'
)
parser.add_argument(
    '--number-of-objects',
    help='Number of objects to transmission.',
    type=int,
    metavar='N',
    required=False
)
parser.add_argument(
    '--profile',
    help='AWS profile locatet in your credentials or config file.',
    required=False
)
parser.add_argument(
    '--region',
    default='eu-central-1',
    help='AWS Region'
)
parser.add_argument(
    '--thread-count',
    help='Number of threads to spawn.',
    default=10
)
parser.add_argument(
    '--verbose',
    '-v',
    help='Increases output of script',
    action='count'
)
cmd_args = parser.parse_args()

BUCKET = cmd_args.bucket
STORAGE_CLASS = cmd_args.storage_class
NUM_OBJ = cmd_args.number_of_objects
PROFILE = cmd_args.profile
AWS_REGION = cmd_args.region
THREAD_COUNT = cmd_args.thread_count
VERBOSE = cmd_args.verbose

if VERBOSE:
    if VERBOSE == 1:
        logger.setLevel(logging.WARNING)
    if VERBOSE == 2:
        logger.setLevel(logging.INFO)
    if VERBOSE >= 3:
        logger.setLevel(logging.DEBUG)


class TransitionObjects(threading.Thread):
    def __init__(self, aws_session, bucket, storage_class):
        threading.Thread.__init__(self)
        self.aws_session = aws_session
        self.bucket = bucket
        self.storage_class = storage_class

    def run(self):
        global q
        self.s3 = self.aws_session.resource('s3')

        while not q.empty():
            self.key = q.get()
            self.dest_obj = self.s3.Object(self.bucket, self.key)
            try:
                if self.dest_obj.storage_class != self.storage_class:
                    self.copy_source = {'Bucket': self.bucket, 'Key': self.key}
                    self.dest_obj.copy_from(
                        self.copy_source,
                        ExtraArgs={'StorageClass': self.storage_class}
                    )
                    q.task_done()
            except ConnectionRefusedError as exc:
                logger.error("To many connections open.\n\
                            Put {} back to queue.".format(self.key))
                q.put(self.key)
            except:
                logger.exception("Unhandeld exception occured.\n\
                            Put {} back to queue.".format(self.key))
                self.cw_put_error()
                q.put(self.key)


def get_s3_objects(aws_session, bucket, num_obj=None):
    logger.warning("Listing {} for objects.".format(bucket))
    keys = list()
    start = time.time()
    try:
        for key in aws_session.resource('s3').Bucket(bucket).objects.all():
            keys.append(key.key)

            if time.time()-1 > start:
                logger.warning("Collected keys: {}".format(len(keys)))
                start = time.time()

            # Break condition to escape earlier thant complete bucket listing
            if num_obj and len(keys) >= num_obj:
                break
        logger.warning("Summary of collected keys {}".format(len(keys)))
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


def main():
    try:
        if PROFILE:
            aws_session = boto3.session.Session(profile_name=PROFILE, region_name=AWS_REGION)
            cred = aws_session.get_credentials().get_frozen_credentials()
        else:
            aws_session = boto3.session.Session(region_name=AWS_REGION)
    except KeyboardInterrupt:
        logger.warning("Exiting...")
        sys.exit(127)
    except ClientError:
        logger.warning("Exiting program. Wrong MFA token!")
        sys.exit(127)
    except ParamValidationError:
        logger.warning("Exiting program. Empty MFA token!")
        sys.exit(127)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        # Getting S3 keys from bucket
        logger.debug("List S3 Keys from {}".format(BUCKET))
        bucket_obj = get_s3_objects(aws_session, BUCKET, num_obj=NUM_OBJ)

        # Sending bucket objects to queue
        [q.put(obj) for obj in bucket_obj]
        q_size = q.qsize()
        logger.warning("{} objects to transmission.".format(q_size))

        if q_size > 0:
            # Check if q_size is less than THREAD_COUNT
            # If so set THREAD_COUNT to thread_count_size
            if q_size < THREAD_COUNT:
                thread_count = q_size
            else:
                thread_count = THREAD_COUNT

        # Starting transmission process
        th = list()
        logger.warning("Starting transmission process.")
        logger.info("Generating {} transmission threads.".format(thread_count))
        for t in range(thread_count):
            th.append(TransitionObjects(aws_session, BUCKET, STORAGE_CLASS))
            th[t].daemon = True
            th[t].start()

        logger.warning("Waiting for transmission queue to be finished.")
        q.join()

        # Waiting for threads to be finished
        for t in range(thread_count):
            th[t].join()

if __name__ == '__main__':
    main()
