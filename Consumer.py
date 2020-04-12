from json import loads
import json
from kafka import KafkaConsumer, TopicPartition
from tqdm import tqdm
from collections import defaultdict
from datetime import *
from itertools import islice
import re
import sys
import argparse
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = 'AKIA3SWS57CWH7EGZPH4'
SECRET_KEY = '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'


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))


def message_number(consumer, partitions):
    consumer.seek_to_end()
    part_messages = dict((p.partition, consumer.position(p)) for p in partitions)
    consumer.seek_to_beginning()
    for p in partitions:
        part_messages[p.partition] -= consumer.position(p)
    return part_messages


def topic_messages(consumer, topic):
    partitions = [TopicPartition(topic, p_id) for p_id in consumer.partitions_for_topic(topic)]
    consumer.assign(partitions)
    part_messages = message_number(consumer, partitions)

    pb = tqdm(total=sum(part_messages.values()), desc="Message gathering")

    while sum(part_messages.values()) > 0:
        message = next(consumer)

        if part_messages[message.partition] > 0:
            pb.update()
            part_messages[message.partition] -= 1
            yield message
    pb.close()


def get_options(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parses command.")
    parser.add_argument("-t", "--task", type=int, help="Task to run, see the description in the homework pdf file")
    parser.add_argument("-n", "--number", type=int, help="A number.")
    parser.add_argument("-o", "--output", help="S3 bucket for the output.")
    options = parser.parse_args(args)
    return options


class Report:

    def __init__(self, task=1, hours_limit=1):
        self.task = task
        self.limit = hours_limit
        self.authors = set()

        self.tweets_by_id = dict()

        # Task 4 20 best tweeters
        self.user_tweets = defaultdict(int)

        # Task 2 10 popular tweeters 10 tweets
        self.author_tweets_id = defaultdict(list)
        self.tweet_id_text = dict()

        # Task 3 aggregated statistics
        self.produced_tweets_per_hour = dict()

        # Task 5 popular hashtags
        self.best_hashtags = defaultdict(int)

        self.TASK_DICT = {4: (self.best_tweeters, self.best_tweeters_to_json),
                          2: (self.popular_tweets, self.popular_tweets_to_json),
                          5: (self.popular_hashtags, self.popular_tweets_to_json),
                          3: (self.aggregated_statistics, self.aggregated_statistics_to_json)}

    def process_author(self, msg):
        self.authors.add(str(msg.value))

    def authors_to_json(self):
        my_json = {"authors": list(self.authors)}
        file_name = 'authors.json'
        with open(file_name, 'w') as f:
            json.dump(my_json, f)
            upload_to_aws(file_name, 'twitter-api-results', file_name)

    def process_tweet(self, msg):
        return self.TASK_DICT[self.task][0](msg)

    def save_to_disk(self):
        return self.TASK_DICT[self.task][1]()

    def best_tweeters(self, msg):
        tweet_data = json.loads(msg.value)
        if datetime.utcfromtimestamp(tweet_data['created_at'] / 1e3) > datetime.now() - timedelta(hours=self.limit):
            self.user_tweets[tweet_data['author_id']] += 1

    def best_tweeters_to_json(self):
        sorted_dict = {k: v for k, v in sorted(self.user_tweets.items(), key=lambda item: item[1], reverse=True)}
        my_json = {'best_tweeters': take(20, sorted_dict)}
        file_name = "best_tweeters.json"
        with open(file_name, 'w') as f:
            json.dump(my_json, f)
            upload_to_aws(file_name, 'twitter-api-results', file_name)

    def popular_tweets(self, msg):
        tweet_data = json.loads(msg.value)
        if datetime.utcfromtimestamp(tweet_data['created_at'] / 1e3) > datetime.now() - timedelta(hours=3):
            self.user_tweets[tweet_data['author_id']] += 1
            self.tweet_id_text[tweet_data['tweet_id']] = tweet_data['text']
            self.author_tweets_id[tweet_data['author_id']].append(tweet_data['tweet_id'])

    def popular_tweets_to_json(self):
        sorted_dict = {k: v for k, v in sorted(self.user_tweets.items(), key=lambda item: item[1], reverse=True)}
        best_tweeters = take(10, sorted_dict)
        user_best_tweets = defaultdict(list)
        for user in best_tweeters:
            for tweet_id in self.author_tweets_id[user]:
                user_best_tweets[user].append(self.tweet_id_text[tweet_id])

        file_name = 'popular_tweets.json'
        with open(file_name, 'w') as f:
            json.dump(user_best_tweets, f)
            upload_to_aws(file_name, 'twitter-api-results', file_name)

    def popular_hashtags(self, msg):
        tweet_data = json.loads(msg.value)
        if datetime.utcfromtimestamp(tweet_data['created_at'] / 1e3) > datetime.now() - timedelta(hours=self.limit):
            hashtags = re.findall(r"#(\w+)", tweet_data['text'])
            for hashtag in hashtags:
                self.best_hashtags[hashtag] += 1

    def hashtags_to_json(self):
        sorted_hashtags = {k: v for k, v in sorted(self.best_hashtags.items(), key=lambda item: item[1], reverse=True)}
        my_json = {'popular_hashtags': take(10, sorted_hashtags)}
        file_name = 'hashtags.json'
        with open(file_name, 'w') as f:
            json.dump(my_json, f)
            upload_to_aws(file_name, 'twitter-api-results', file_name)

    def aggregated_statistics(self, msg):
        tweet_data = json.loads(msg.value)
        for time_gap in range(3):
            creation_time = datetime.utcfromtimestamp(tweet_data['created_at'] / 1e3)
            current_time = datetime.now()
            author = tweet_data['author_id']
            if (creation_time > (current_time - timedelta(hours=time_gap + 1))) and (
                    not creation_time >= (current_time - timedelta(hours=time_gap - 1))):
                if author not in self.produced_tweets_per_hour:
                    self.produced_tweets_per_hour[author] = [0, 0, 0]
                self.produced_tweets_per_hour[author][time_gap] += 1

    def aggregated_statistics_to_json(self):
        file_name = 'aggregated_statistics.json'
        with open(file_name, 'w') as f:
            json.dump(self.produced_tweets_per_hour, f)
            upload_to_aws(file_name, 'twitter-api-results', file_name)


if __name__ == "__main__":
    options = get_options(sys.argv[1:])
    task = options.task
    c = KafkaConsumer(bootstrap_servers=['ec2-3-132-131-17.us-east-2.compute.amazonaws.com:9092'],
                      auto_offset_reset='earliest',
                      enable_auto_commit=False,
                      group_id='newgroup_01',
                      client_id="client-1",
                      value_deserializer=lambda x: loads(x.decode('utf-8')),
                      session_timeout_ms=6000)

    report = Report(task=task, hours_limit=options.number)
    if task == 1:
        for message in topic_messages(c, 'accounts'):
            report.process_author(message)
        report.authors_to_json()
    else:
        for message in topic_messages(c, "tweets"):
            report.process_tweet(message)
        report.save_to_disk()
