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


class ElasticDump:
    def __init__(self):
        """
        Init elastic dumper
        """
        self.__config = self.load_config()
        p_config = {}
        for options in self.__config.values():
            p_config.update(**options)
        self.__config = p_config
        self.__client = Elasticsearch(hosts=[f"{self.__config.get('host')}:{self.__config.get('port')}"])

        Path(self.__config.get('directory')).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def load_config(config_file: str = "config.yaml") -> dict:
        """
        Load the configuration
        :param config_file: configuration YAML file
        :return: dictionary with config
        """
        with open(file=config_file, mode="r") as config:
            return safe_load(config)

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
        with open(file=Path(self.__config.get('directory')).joinpath(filename), mode="w") as output_file:
            dump(results, output_file)

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
    dumper = ElasticDump()
    dumper.run()
