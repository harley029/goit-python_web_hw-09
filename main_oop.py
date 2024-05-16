import json
import logging
import time
import requests
from bs4 import BeautifulSoup
from mongoengine import connect
from models import Author, Quote
from abc import ABC, abstractmethod

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DatabaseAbstract(ABC):
    @abstractmethod
    def connect_to_db(self):
        pass

    @abstractmethod
    def upload_author_data(self, authors_data: list):
        pass

    @abstractmethod
    def upload_quotes_data(self, quotes_data: list):
        pass


class Database(DatabaseAbstract):
    def __init__(self, db_name, db_host):
        self.db_name = db_name
        self.db_host = db_host
        self.connect_to_db()

    def connect_to_db(self):
        logging.info("Підключення до бази даних.")
        connect(self.db_name, host=self.db_host)

    def upload_author_data(self, authors_data: list):
        logging.info("Збереження авторів у базі даних.")
        for author_data in authors_data:
            author = Author(**author_data)
            author.save()
        logging.info("Збереження авторів завершено.")

    def upload_quotes_data(self, quotes_data: list):
        logging.info("Збереження цитат у базі даних.")
        for quote_data in quotes_data:
            author_name = quote_data["author"]
            author = Author.objects(fullname=author_name).first()
            if author:
                quote_data["author"] = author
                quote = Quote(**quote_data)
                quote.save()
        logging.info("Збереження цитат завершено.")


class SoupFetcher:
    @staticmethod
    def get_soup(url):
        logging.info(f"Отримання вмісту сторінки: {url}")
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "lxml")
        return soup


class JsonFileHandlerAbstract(ABC):
    @abstractmethod
    def write_to_json(self, filename: str, data: list):
        pass

    @abstractmethod
    def read_from_json(self, filename: str):
        pass


class JsonFileHandler(JsonFileHandlerAbstract):
    @staticmethod
    def write_to_json(filename: str, data: list):
        logging.info(f"Запис даних у файл: {filename}")
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @staticmethod
    def read_from_json(filename: str):
        logging.info(f"Читання даних з файлу: {filename}")
        with open(filename) as f:
            data = json.load(f)
        return data


class Scraper(ABC):
    @abstractmethod
    def get_pages(self) -> list:
        pass

    @abstractmethod
    def parse_data(self):
        pass


class AuthorScraper(Scraper):
    def __init__(self, base_url, json_handler: JsonFileHandler):
        self.base_url = base_url
        self.json_handler = json_handler

    def get_pages(self) -> list:
        logging.info(f"Отримання списку сторінок для парсингу з: {self.base_url}")
        links = [self.base_url]
        link_to_parse = self.base_url
        while link_to_parse:
            soup = SoupFetcher.get_soup(link_to_parse)
            link = soup.find("li", class_="next")
            if link:
                link = link.find("a").attrs.get("href", "")
                links.append(self.base_url + link)
                link_to_parse = self.base_url + link
            else:
                link_to_parse = None
        logging.info(f"Знайдено {len(links)} сторінок для парсингу.")
        return links

    def get_authors_info(self, urls: list) -> list:
        logging.info("Отримання інформації про авторів.")
        author_links = []
        for url in urls:
            soup = SoupFetcher.get_soup(url)
            authors = soup.find_all("small", class_="author")
            authors_description = soup.find_all("div", class_="quote")
            for el in range(len(authors)):
                author_name = authors[el].text
                author_found = any(
                    author["author_name"] == author_name for author in author_links
                )
                if not author_found:
                    author_info_url = self.base_url + authors_description[el].find(
                        "a", class_=""
                    ).attrs.get("href", "")
                    author_links.append(
                        {"author_name": author_name, "author_info_url": author_info_url}
                    )
        logging.info(f"Знайдено {len(author_links)} авторів.")
        return author_links

    def parse_data(self, author_links: list):
        logging.info("Парсинг інформації про авторів.")
        author_list = []
        for author in author_links:
            url = author["author_info_url"]
            soup = SoupFetcher.get_soup(url)
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
        logging.info(f"Парсинг завершено. Знайдено {len(author_list)} авторів.")
        return author_list


class QuoteScraper(Scraper):
    def __init__(self, base_url, db: Database, json_handler: JsonFileHandler):
        self.base_url = base_url
        self.db = db
        self.json_handler = json_handler

    def get_pages(self) -> list:
        logging.info(f"Отримання списку сторінок для парсингу з: {self.base_url}")
        links = [self.base_url]
        link_to_parse = self.base_url
        while link_to_parse:
            soup = SoupFetcher.get_soup(link_to_parse)
            link = soup.find("li", class_="next")
            if link:
                link = link.find("a").attrs.get("href", "")
                links.append(self.base_url + link)
                link_to_parse = self.base_url + link
            else:
                link_to_parse = None
        logging.info(f"Знайдено {len(links)} сторінок для парсингу.")
        return links

    def parse_data(self, links: list):
        logging.info("Парсинг цитат.")
        quotes_list = []
        for url in links:
            soup = SoupFetcher.get_soup(url)
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
        logging.info(f"Парсинг завершено. Знайдено {len(quotes_list)} цитат.")
        return quotes_list


class ScraperAbstract(ABC):
    @abstractmethod
    def scrape_and_store(self):
        pass


class ScraperManager(ScraperAbstract):
    def __init__(self, scraper: Scraper):
        self.scraper = scraper

    def scrape_and_store(self):
        start_time = time.time()

        # Отримання інформації про авторів з послідуючим парсингом та записом у json файл
        author_scraper = AuthorScraper(self.scraper.base_url, self.scraper.json_handler)
        pages = author_scraper.get_pages()
        author_links = author_scraper.get_authors_info(pages)
        authors_info = author_scraper.parse_data(author_links)
        self.scraper.json_handler.write_to_json("authors.json", authors_info)

        # Парсинг всіх цитат з отриманого списку сторінок з послідуючим записом у json файл
        quotes_info = self.scraper.parse_data(pages)
        self.scraper.json_handler.write_to_json("quotes.json", quotes_info)

        # Зчитування даних з файлів json
        authors_data = self.scraper.json_handler.read_from_json("authors.json")
        quotes_data = self.scraper.json_handler.read_from_json("quotes.json")

        # Завантаження зчитаних даних у MongoDB
        self.scraper.db.upload_author_data(authors_data)
        self.scraper.db.upload_quotes_data(quotes_data)

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Скрипт завершено успішно за {elapsed_time:.2f} секунд.")


if __name__ == "__main__":
    BASE_URL = "https://quotes.toscrape.com"
    DB_NAME = "book_db"
    DB_HOST = "mongodb+srv://harley029:8lyrMibko@cluster0.b6lozo9.mongodb.net/?retryWrites=true&w=majority"

    db = Database(DB_NAME, DB_HOST)
    json_handler = JsonFileHandler()
    scraper = QuoteScraper(BASE_URL, db, json_handler)

    scraper_manager = ScraperManager(scraper)
    scraper_manager.scrape_and_store()