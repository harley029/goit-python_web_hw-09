import json
import logging
import time
from abc import ABC, abstractmethod

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from mongoengine import connect
from models import Author, Quote

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DatabaseAbstract(ABC):
    @abstractmethod
    async def connect_to_db(self):
        pass

    @abstractmethod
    async def upload_author_data(self, authors_data: list):
        pass

    @abstractmethod
    async def upload_quotes_data(self, quotes_data: list):
        pass


class Database(DatabaseAbstract):
    def __init__(self, db_name, db_host):
        self.db_name = db_name
        self.db_host = db_host

    async def connect_to_db(self):
        logging.info("Підключення до бази даних.")
        connect(self.db_name, host=self.db_host)

    async def upload_author_data(self, authors_data: list):
        logging.info("Збереження авторів у базі даних.")
        for author_data in authors_data:
            # Перевірка наявності автора в базі даних
            if not Author.objects(fullname=author_data["fullname"]).first():
                author = Author(**author_data)
                author.save()
        logging.info("Збереження авторів завершено.")

    async def upload_quotes_data(self, quotes_data: list):
        logging.info("Збереження цитат у базі даних.")
        for quote_data in quotes_data:
            author_name = quote_data["author"]
            author = Author.objects(fullname=author_name).first()
            if author:
                quote_data["author"] = author
                # Перевірка наявності цитати в базі даних
                if not Quote.objects(quote=quote_data["quote"]).first():
                    quote = Quote(**quote_data)
                    quote.save()
        logging.info("Збереження цитат завершено.")


class SoupFetcher:
    @staticmethod
    async def get_soup(session, url):
        logging.info("Отримання вмісту сторінки: %s", url)
        async with session.get(url) as response:
            text = await response.text()
            soup = BeautifulSoup(text, "lxml")
        return soup


class JsonFileHandlerAbstract(ABC):
    @abstractmethod
    async def write_to_json(self, filename: str, data: list):
        pass

    @abstractmethod
    async def read_from_json(self, filename: str):
        pass


class JsonFileHandler(JsonFileHandlerAbstract):
    @staticmethod
    async def write_to_json(filename: str, data: list):
        logging.info("Запис даних у файл: %s", filename)
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @staticmethod
    async def read_from_json(filename: str):
        logging.info("Читання даних з файлу: %s", filename)
        with open(filename) as f:
            data = json.load(f)
        return data


class Scraper(ABC):
    @abstractmethod
    async def get_pages(self, session) -> list:
        pass

    @abstractmethod
    async def parse_data(self, session):
        pass


class AuthorScraper(Scraper):
    def __init__(self, base_url):
        self.base_url = base_url

    async def get_pages(self, session) -> list:
        logging.info("Отримання списку сторінок для парсингу з: %s", self.base_url)
        links = [self.base_url]
        link_to_parse = self.base_url
        while link_to_parse:
            soup = await SoupFetcher.get_soup(session, link_to_parse)
            link = soup.find("li", class_="next")
            if link:
                link = link.find("a").attrs.get("href", "")
                links.append(self.base_url + link)
                link_to_parse = self.base_url + link
            else:
                link_to_parse = None
        logging.info("Знайдено %d сторінок для парсингу.", len(links))
        return links

    async def get_authors_info(self, session, urls: list) -> list:
        logging.info("Отримання інформації про авторів.")
        author_links = []
        for url in urls:
            soup = await SoupFetcher.get_soup(session, url)
            authors = soup.find_all("small", class_="author")
            authors_description = soup.find_all("div", class_="quote")
            for el, author in enumerate(authors):
                author_name = author.text
                author_found = any(
                    author_link["author_name"] == author_name
                    for author_link in author_links
                )
                if not author_found:
                    author_info_url = self.base_url + authors_description[el].find(
                        "a", class_=""
                    ).attrs.get("href", "")
                    author_links.append(
                        {"author_name": author_name, "author_info_url": author_info_url}
                    )
        logging.info("Знайдено %d авторів.", len(author_links))
        return author_links

    async def parse_data(self, session, author_links: list):
        logging.info("Парсинг інформації про авторів.")
        author_list = []
        for author in author_links:
            url = author["author_info_url"]
            soup = await SoupFetcher.get_soup(session, url)
            fullname = soup.find("h3", class_="author-title").text.strip()
            born_date = soup.find("span", class_="author-born-date").text.strip()
            born_location = soup.find(
                "span", class_="author-born-location"
            ).text.strip()
            description = soup.find("div", class_="author-description").text.strip()
            author_list.append(
                {
                    "fullname": fullname,
                    "born_date": born_date,
                    "born_location": born_location,
                    "description": description,
                }
            )
        logging.info("Парсинг завершено. Знайдено %d авторів.", len(author_list))
        return author_list


class QuoteScraper(Scraper):
    def __init__(self, base_url, db: Database):
        self.base_url = base_url
        self.db = db
        self.json_handler = JsonFileHandler()

    async def get_pages(self, session) -> list:
        logging.info("Отримання списку сторінок для парсингу з: %s", self.base_url)
        links = [self.base_url]
        link_to_parse = self.base_url
        while link_to_parse:
            soup = await SoupFetcher.get_soup(session, link_to_parse)
            link = soup.find("li", class_="next")
            if link:
                link = link.find("a").attrs.get("href", "")
                links.append(self.base_url + link)
                link_to_parse = self.base_url + link
            else:
                link_to_parse = None
        logging.info("Знайдено %d сторінок для парсингу.", len(links))
        return links

    async def parse_data(self, session, links: list):
        logging.info("Парсинг цитат.")
        quotes_list = []
        for url in links:
            soup = await SoupFetcher.get_soup(session, url)
            quotes = soup.find_all("div", class_="quote")
            for el in quotes:
                author = el.find("small", class_="author").text
                quote = el.find("span", class_="text").text
                tags_list = [
                    tag.text.strip()
                    for tag in el.find("div", class_="tags").find_all("a", class_="tag")
                ]
                quotes_list.append(
                    {
                        "tags": tags_list,
                        "author": author,
                        "quote": quote,
                    }
                )
        logging.info("Парсинг завершено. Знайдено %d цитат.", len(quotes_list))
        return quotes_list


class ScraperAbstract(ABC):
    @abstractmethod
    async def scrape_and_store(self):
        pass


class ScraperManager(ScraperAbstract):
    def __init__(self, scraper: Scraper, json_handler: JsonFileHandler):
        self.scraper = scraper
        self.json_handler = json_handler

    async def scrape_and_store(self):
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            # Отримання інформації про авторів з послідуючим парсингом та записом у json файл
            author_scraper = AuthorScraper(self.scraper.base_url)
            pages = await author_scraper.get_pages(session)
            author_links = await author_scraper.get_authors_info(session, pages)
            authors_info = await author_scraper.parse_data(session, author_links)
            await self.scraper.json_handler.write_to_json("authors.json", authors_info)

            # Парсинг всіх цитат з отриманого списку сторінок з послідуючим записом у json файл
            quotes_info = await self.scraper.parse_data(session, pages)
            await self.scraper.json_handler.write_to_json("quotes.json", quotes_info)

        # Зчитування даних з файлів json
        authors_data = await self.scraper.json_handler.read_from_json("authors.json")
        quotes_data = await self.scraper.json_handler.read_from_json("quotes.json")

        # Завантаження зчитаних даних у MongoDB
        await self.scraper.db.connect_to_db()
        await self.scraper.db.upload_author_data(authors_data)
        await self.scraper.db.upload_quotes_data(quotes_data)

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info("Скрипт завершено успішно за %.2f секунд.", elapsed_time)


if __name__ == "__main__":
    BASE_URL = "https://quotes.toscrape.com"
    DB_NAME = "book_db"
    DB_HOST = "mongodb+srv://harley029:8lyrMibko@cluster0.b6lozo9.mongodb.net/?retryWrites=true&w=majority"

    db = Database(DB_NAME, DB_HOST)
    json_handler = JsonFileHandler()
    scraper = QuoteScraper(BASE_URL, db)

    scraper_manager = ScraperManager(scraper, json_handler)
    asyncio.run(scraper_manager.scrape_and_store())
