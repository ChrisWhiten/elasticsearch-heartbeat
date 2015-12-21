import json
from pprint import pprint
from elasticsearch import Elasticsearch
from slacker import Slacker

def updateCounts(indices):
  print("updating counts...")

  updated_indices = []
  for index in indices:
    updated_index = index

    es = Elasticsearch([
      {"host": index["host"], "port": index["port"]}
    ])
    count_descriptor = es.count(
      index = index["index"],
      doc_type = index["type"]
    )

    count = count_descriptor["count"]
    prev_count = index["count"]
    updated_index["count"] = count

    if count == prev_count:
      print("No new events for %s (%s)" % (index["name"], index["index"]))

      retry_count = index.get("retry_count", 0)
      updated_index["retry_count"] = retry_count + 1
    else:
      updated_index["retry_count"] = 0

    updated_indices.append(updated_index)

  return updated_indices

def notify(setup, updated_indices):
  print("notifying...")

  # right now, this is just set to post into a slack channel.
  max_retry = setup["max_retry_before_notify"]
  for index in updated_indices:
    if index["retry_count"] == max_retry:
      slack = Slacker(setup["slack_api_token"])
      slack.chat.post_message(setup["slack_channel"], "Index %s (%s) has not added any new documents within the last %d checks" % (index["name"], index["index"], max_retry))


def storeResults(setup, updated_indices):
  print("storing results...")

  new_setup = setup
  new_setup["indices"] = updated_indices

  with open("setup.json", "wb") as outfile:
    json.dump(new_setup, outfile)

def loadSetup():
  print("loading...")

  with open("setup.json") as setup_file:
    setup = json.load(setup_file)
    return setup

if __name__ == "__main__":
  setup = loadSetup()
  updated_indices = updateCounts(setup["indices"])
  storeResults(setup, updated_indices)
  notify(setup, updated_indices)