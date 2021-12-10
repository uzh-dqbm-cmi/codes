"""
Author: @matteobe
"""

from typing import List, Union, Callable
import requests
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


class ICD10:

    def __init__(self, version: int = 2019):
        """
        Initialize the ICD10 codes retrieval with the correct version

        Args:
            version (int): the ICD10 version to use. Can be chosen from 2019, 2016, 2015, 2014, 2010, 2008.
                Default is 2019
        """

        self.version = str(version)
        self.url = f"https://icd.who.int/browse10/{self.version}/en"
        self.webdriver = "/Users/matteoberchier/Downloads/chromedriver"
        self.max_processes = 20

    def multi_thread(self, func: Callable, values: Union[List[str], pd.Series]) -> pd.DataFrame:
        """
        Execute the function in multi-threading mode and concat the resulting dataframe

        Args:
            func (Callable): a function which takes only one value
            values (pd.Series, List[str]): series / list containing the values to be passed to the function

        Returns:
             df (pd.DataFrame): frame with concatenated return values from the function
        """

        if type(values) == pd.Series:
            values = values.to_list()

        processes = min(len(values), self.max_processes)
        with ThreadPool(processes=processes) as pool:
            results = pool.map(func, values)

        return pd.concat(results)

    def multi_thread2(self, func: Callable, values) -> pd.DataFrame:
        processes = list()
        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            for value in values:
                processes.append(executor.submit(func, value))

        results = list()
        for task in as_completed(processes):
            results.append(task.result())

        return pd.concat(results)

    def chapters(self) -> pd.DataFrame:
        """
        Retrieve the ICD10 chapters

        Returns:
            chapters (pd.DataFrame): frame with chapter codes and descriptions
        """

        chrome = Service(self.webdriver)
        driver = webdriver.Chrome(service=chrome, options=webdriver.ChromeOptions())
        driver.implicitly_wait(0.01)

        driver.get(self.url)
        hierarchy = driver.find_elements(By.ID, "ygtvc1")[0]
        chapters = hierarchy.find_elements(By.CLASS_NAME, "ygtvitem")

        df = list()
        for chapter in chapters:
            label = chapter.find_element(By.CLASS_NAME, "ygtvlabel  ")
            code = label.find_element(By.CLASS_NAME, "icode ").text
            data = {
                'chapter:code': code,
                'chapter:description': label.text[len(code) + 1:],
            }
            df.append(pd.DataFrame(data=data, index=[0]))

        return pd.concat(df)

    def blocks(self, chapters: Union[List[str], pd.Series]) -> pd.DataFrame:
        """
        Retrieve all the blocks in a list of chapters codes

        Args:
            chapters(pd.Series, List[str]): series / list containing the chapter codes for which to retrieve the
                blocks

        Returns:
             blocks (pd.DataFrame): frame with chapters codes, block codes and descriptions
        """
        return self.multi_thread(func=self.__blocks, values=chapters)

    def __blocks(self, chapter: str) -> pd.DataFrame:
        """
        Retrieve the blocks from a chapter code

        Args:
            chapter (str): chapter code for which to retrieve the blocks

        Returns:
            blocks (pd.DataFrame): frame with chapter code, blocks codes and descriptions
        """

        url = f"https://icd.who.int/browse10/{self.version}/en/GetConcept?ConceptId={chapter}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, features="lxml")
        blocks = soup.find_all("li", {"class": "Blocklist1"})

        df = list()
        for block in blocks:
            code = block.find("a", {"class": "code"}).text
            label = block.find("span", {"class": "label"}).text \
                .replace("\r", "").replace("\n", "")
            data = {
                'chapter:code': chapter,
                'block:code': code,
                'block:description': label
            }
            df.append(pd.DataFrame(data=data, index=[0]))

        return pd.concat(df)

    def categories(self, blocks: Union[List[str], pd.Series]) -> pd.DataFrame:
        """
        Retrieve all the categories in a list of block codes

        Args:
            blocks (pd.Series, List[str]): series / list containing the block codes for which to retrieve the
                categories

        Returns:
             categories (pd.DataFrame): frame with block codes, categories codes and descriptions
        """
        return self.multi_thread2(func=self.__categories, values=blocks)

    def __categories(self, block: str) -> pd.DataFrame:
        """
        Retrieve the ICD10 categories in a block

        Args:
            block (str): block code for which to retrieve the categories

        Returns:
            categories (pd.DataFrame): frame with block codes, categories codes and descriptions
        """

        url = f"https://icd.who.int/browse10/{self.version}/en/JsonGetChildrenConcepts?" \
              f"ConceptId={block}&useHtml=true&showAdoptedChildren=true"
        result = requests.get(url)

        df = list()
        if result.status_code != 200:
            print(result.status_code)
            data = {
                'block:code': block,
                'category:code': result.status_code,
                'category:description': result.reason
            }
            df.append(pd.DataFrame(data=data, index=[0]))
        else:
            categories = result.json()
            for category in categories:
                code = category['ID']
                html = BeautifulSoup(category['html'], features="lxml")
                description = html.find("a", {"class": "ygtvlabel"}).text \
                    .replace("\r", "").replace("\n", "")[len(code):]
                data = {
                    'block:code': block,
                    'category:code': code,
                    'category:description': description
                }
                df.append(pd.DataFrame(data=data, index=[0]))

        return pd.concat(df)
