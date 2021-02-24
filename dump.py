#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from json import dump
from pathlib import Path
from time import time
from typing import Union

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from yaml import safe_load


class Defaults:
    QUERY = {"query": {"match_all": {}}}


def load_config(config_file: str = "config.yaml") -> dict:
    """
    Load the configuration
    :param config_file: configuration YAML file
    :return: dictionary with config
    """
    with open(file=config_file, mode="r") as config_file:
        config_yaml = safe_load(config_file)
    flat_config = {}
    for options in config_yaml.values():
        flat_config.update(**options)
    return flat_config


class ElasticDump:
    def __init__(self, config: dict):
        """
        Init dumper
        :param config: config
        """
        self.__host = config.get("host")
        self.__port = config.get("port")
        self.__config = config

        self.__path = Path(self.__config.get("directory")).joinpath(self.__host.replace(".", "_"))
        self.__path.mkdir(parents=True, exist_ok=True)

        self.__client = Elasticsearch(hosts=[f"{self.__host}:{self.__port}"])

    def get_indices(self, index: str = "*") -> list:
        """
        Return all available indices
        :param index: Particular index to use (wildcard by default, all)
        :return: list with indices only
        """
        indices = self.__client.indices.get(index=index)
        return [index for index in indices.keys()]

    def dump_to_file(self, results: Union[list, dict], filename: str) -> None:
        """
        Dump results to the output JSON file
        :param results: results to dump
        :param filename: filename
        :return: None
        """
        with open(file=self.__path.joinpath(filename), mode="w") as output_file:
            dump(results, output_file, indent=2)

    def process_index(self, index_name: str) -> None:
        """
        Process and save the current index
        :param index_name: index name to process
        :return: None
        """
        # Yields hits, one hit per time (not batch)
        generator = scan(
            self.__client,
            query=Defaults.QUERY,
            scroll=self.__config.get('scroll'),
            size=self.__config.get('size'),
            index=index_name
        )
        results = list(generator)
        print(f"Index: {index_name}, documents: {len(results)}")
        self.dump_to_file(results, filename=f"{index_name}_{int(time())}.json")

    def run(self) -> None:
        """
        Run with workers
        :return: None
        """
        indices = self.get_indices()
        with ThreadPoolExecutor(max_workers=self.__config.get('workers')) as executor:
            for es_index in indices:
                executor.submit(dumper.process_index, es_index)
            executor.shutdown(wait=True)


if __name__ == "__main__":
    config = load_config()
    dumper = ElasticDump(config)
    dumper.run()
