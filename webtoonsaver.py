import os
import re
import asyncio
import aiohttp
import requests
import multiprocess as mp

from shutil import rmtree
from slugify import slugify
from bs4 import BeautifulSoup
from PIL import Image, ImageFile
from collections import OrderedDict
from aiofiles import open as aio_open
from PIL import Image, UnidentifiedImageError
from concurrent.futures import ThreadPoolExecutor

ImageFile.LOAD_TRUNCATED_IMAGES = True


class WebtoonSaver:
    def __init__(
        self,
        url: str,
        name: str | None = None,
        save_path: str | None = None,
        num_chapters: int | None = None,
        chapter_range: dict | None = {"start": None, "end": None},
    ):
        """
        A class to save comics from a given URL to a specified path.

        Args:
            url (str): The URL of the comic.
            name (str, optional): The name of the comic. If not provided, it is extracted from the URL.
            save_path (str, optional): The path where the comic will be saved. If not provided, it defaults to '~/Desktop/Comics/<comic_name>/'.
            num_chapters (int, optional): The number of chapters to download. Overrides chapter_range if provided.
            chapter_range (dict, optional): A dictionary with 'start' and 'end' keys to specify the range of chapters to download.

        Attributes:
            url (str): The URL of the comic.
            comic_name (str): The slugified name of the comic.
            save_path (str): The path where the comic will be saved.
            chapter_start (int | None): The starting chapter number.
            chapter_end (int | None): The ending chapter number.
        """
        if "webtoonscan.com" in url:
            self.database = {
                "chapter_class": "wp-manga-chapter",
                "image_class": "wp-manga-chapter-img",
            }
        elif "manhwa18.cc" in url:
            url = url + "/"
            self.database = {
                "chapter_class": "a-h wleft",
                "image_class": re.compile(r"loading p\d+"),
            }

        self.url = url
        if name is not None:
            self.comic_name = name
        else:
            self.comic_name = self.url.rsplit("/", 2)[1]
        self.comic_name = slugify(self.comic_name)

        if save_path is not None:
            self.save_path = save_path + f"{self.comic_name}/"
        else:
            self.save_path = os.path.expanduser(f"~/Desktop/Comics/{self.comic_name}/")

        if not (os.path.isdir(self.save_path)):
            os.makedirs(self.save_path)

        self.chapter_start = chapter_range["start"]
        self.chapter_end = chapter_range["end"]

        if num_chapters is not None:
            self.chapter_start = 1
            self.chapter_end = num_chapters

    # Get all the chapters url for the given comic url
    def getChapterURLs(self):
        """
        Retrieves URLs for each chapter of the comic from the provided URL.

        Returns:
            OrderedDict: A dictionary where keys are chapter IDs and values are chapter URLs.
        """
        r = requests.get(self.url)
        soup = BeautifulSoup(r.text, "html.parser")
        self.chapter_urls = OrderedDict()

        for chapter_element in soup.findAll(
            "li", {"class": self.database["chapter_class"]}
        ):
            chapter_url = chapter_element.find("a")["href"]
            chapter_id = re.findall(r"\d+", chapter_url)[0]
            if "webtoonscan.com" in self.url:
                self.chapter_urls[chapter_id] = chapter_url
            elif "manhwa18.cc" in self.url:
                self.chapter_urls[chapter_id] = self.url.rsplit("/", 3)[0] + chapter_url

        # Print the chapters that weren't parsed
        last_chapter = int(list(self.chapter_urls.keys())[0])
        first_chapter = int(list(self.chapter_urls.keys())[-1])
        missing_chap = last_chapter - len(self.chapter_urls.keys())
        if missing_chap > 0:
            expected_list = [i for i in range(first_chapter, last_chapter + 1)]
            chapter_list = [int(i) for i in self.chapter_urls.keys()]
            print(
                f"{missing_chap} missing chapter/s for {self.comic_name} comic on {self.url} \n{list(set(expected_list).difference(chapter_list))}"
            )

        if self.chapter_start:
            self.chapter_urls = {
                chapter_id: chapter_url
                for chapter_id, chapter_url in self.chapter_urls.items()
                if int(chapter_id) >= self.chapter_start
            }

        if self.chapter_end:
            self.chapter_urls = {
                chapter_id: chapter_url
                for chapter_id, chapter_url in self.chapter_urls.items()
                if int(chapter_id) <= self.chapter_end
            }

    def atoi(self, text: str):
        """
        Convert a string of digits to an integer.

        Args:
            text (str): The string to convert.

        Returns:
            int or str: The converted integer if the input is all digits, otherwise the original string.
        """
        return int(text) if text.isdigit() else text

    def natural_keys(self, text: str):
        """
        Sort key function for natural sorting.

        Args:
            text (str): The text to be sorted.

        Returns:
            list: A list containing integers and strings for natural sorting.
        """
        return [self.atoi(c) for c in re.split(r"(\d+)", text)]

    async def download_image(
        self, session: aiohttp.ClientSession, url: str, idx: int, image_save_path: str
    ):
        """
        Download an image from the provided URL asynchronously.

        Args:
            session (aiohttp.ClientSession): The aiohttp client session.
            url (str): The URL of the image to download.
            idx (int): The index of the image in the comic.
            image_save_path (str): The path where the downloaded image will be saved.

        Returns:
            int: The index of the downloaded image.
        """
        async with session.get(url) as response:
            content = await response.read()
            async with aio_open(f"{image_save_path}/images{idx+1}.jpg", "wb") as f:
                await f.write(content)
            return idx

    async def download_images(self, image_urls: list, image_save_path: str):
        """
        Download multiple images asynchronously from the provided URLs.

        Args:
            image_urls (list): A list of URLs of the images to download.
            image_save_path (str): The path where the downloaded images will be saved.

        Returns:
            list: A list of integers representing the indices of the downloaded images.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.download_image(session, url, idx, image_save_path)
                for idx, url in enumerate(image_urls)
            ]
            return await asyncio.gather(*tasks)

    async def process_chapter(self, chapter_id: str, chapter_url: str, save_path: str):
        """
        Process a chapter of the comic, download images, and save as a PDF.

        Args:
            chapter_id (str): The ID of the chapter.
            chapter_url (str): The URL of the chapter.
            save_path (str): The path where the PDF will be saved.

        Returns:
            None
        """
        image_save_path = save_path + f"Chapter{chapter_id}_Images/"
        pdf_filename = save_path + f"Chapter-{chapter_id}.pdf"

        if not os.path.isfile(pdf_filename):
            if os.path.isdir(image_save_path):
                rmtree(image_save_path)
            os.mkdir(image_save_path)

            r = requests.get(chapter_url)
            soup = BeautifulSoup(r.text, "html.parser")

            images = soup.findAll("img", {"class": self.database["image_class"]})
            image_urls = [re.sub("\s+", "", i["src"]) for i in images]

            downloaded_indexes = await self.download_images(image_urls, image_save_path)

            if len(downloaded_indexes) != len(image_urls):
                print(
                    f"Total {len(downloaded_indexes)} Images Downloaded Out of {len(image_urls)} for Chapter {chapter_id}"
                )

            alist = os.listdir(image_save_path)
            alist.sort(key=self.natural_keys)
            imagelist = [image_save_path + i for i in alist if ".jpg" in i]

            im_list = []

            def load_image(image_path):
                try:
                    img = Image.open(image_path)
                    if img.height > 500:
                        if img.mode == "RGBA":
                            img = img.convert("RGB")
                        return img
                except UnidentifiedImageError:
                    return None

            with ThreadPoolExecutor() as executor:
                im_list = list(executor.map(load_image, imagelist))

            im_list = [img for img in im_list if img is not None]

            if im_list:
                im_list[0].save(
                    pdf_filename,
                    "PDF",
                    resolution=100.0,
                    save_all=True,
                    append_images=im_list[1:],
                )

            rmtree(image_save_path)

    def run_process_chapter(self, chapter_id: str, chapter_url: str, save_path: str):
        """
        Asynchronously run the process of downloading and saving a chapter of the comic as a PDF.

        Args:
            chapter_id (str): The ID of the chapter.
            chapter_url (str): The URL of the chapter.
            save_path (str): The path where the PDF will be saved.

        Returns:
            None
        """
        asyncio.run(self.process_chapter(chapter_id, chapter_url, save_path))


def run_webtoonsaver(
    url: str,
    num_chapters: int | None = None,
    chapter_range: dict | None = {"start": None, "end": None},
    n_workers: int = -1,
):
    """
    Main function to download and save chapters of a comic.

    Args:
        url (str): The URL of the comic.
        num_chapters (int, optional): The number of chapters to download. Overrides chapter_range if provided.
        chapter_range (dict, optional): A dictionary with 'start' and 'end' keys to specify the range of chapters to download. Defaults to {"start": None, "end": None}.
        n_workers (int, optional): The number of worker processes to use. Defaults to -1, which sets the number of workers to the number of available CPU cores.

    Returns:
        None
    """
    comic_obj = WebtoonSaver(
        url=url, num_chapters=num_chapters, chapter_range=chapter_range
    )
    comic_obj.getChapterURLs()
    chapter_urls = list(comic_obj.chapter_urls.items())

    tasks = [
        (chapter_id, chapter_url, comic_obj.save_path)
        for chapter_id, chapter_url in chapter_urls
    ]

    if n_workers == -1:
        n_workers = mp.cpu_count()
    else:
        if n_workers > mp.cpu_count():
            n_workers = mp.cpu_count()

    with mp.Pool(n_workers) as p:
        p.starmap(comic_obj.run_process_chapter, tasks)
