import math
import re
import aiohttp
import asyncio
import json
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


class ProductScraper:

    def __init__(self, urls):
        self.urls = urls
        self.products_data = []
        self.exception_tasks = set()
        self.SEM_LIMIT = 7
        self.exception_tasks_categories = set()

    async def get_product_data(self, session, url, page):
        try:
            full_url = f"{url}?p={page}"
            async with session.get(url=full_url) as response:
                soup = BeautifulSoup(await response.text(), "lxml")
                articles = soup.find_all('article')
                for article in articles:
                    price_meta = article.find('meta', itemprop='price')
                    price = price_meta['content'] if price_meta else None

                    brand_meta = article.find('span', itemprop='brand').find('meta', itemprop='name')
                    brand = brand_meta['content'] if brand_meta else None

                    name_meta = article.find_all('meta', itemprop='name')
                    product_name = name_meta[1]['content'] if ['content'] else None

                    availability_meta = article.find('meta', itemprop='availability')
                    in_stock = availability_meta[
                                   'content'] == 'https://schema.org/InStock' if availability_meta else False

                    sku_meta = article.find('meta', itemprop='sku')
                    product_id = sku_meta['content'] if sku_meta else None

                    type = article.find('span', itemprop='brand').find('meta',
                                                                       itemprop='name').parent.parent.find_previous().find_previous().text.strip()

                    images = article.find_all('source', type='image/jpeg')
                    image_urls = [img['srcset'] for img in images]

                    self.products_data.append({
                        "brand": brand,
                        "name": product_name,
                        "price": price,
                        "in_stock": in_stock,
                        "id": product_id,
                        "type": type,
                        "photos": image_urls,
                    })
        except Exception as e:
            self.exception_tasks.add((url, page))
          #  print(f"[ERROR] Ошибка на {url} и {page}: {e}")

    async def gather_data(self, url, sem):

        async with sem:
            try:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    async with session.get(url=url) as response:
                        html = await response.text()
                        soup = BeautifulSoup(html, "lxml")
                        product_count_span = soup.find('span', attrs={'data-category-products-count': True})
                        if product_count_span:
                            text = product_count_span.get_text(strip=True)
                            match = re.search(r'(\d[\d\s]*)\s+продукт', text)
                            if match:
                                product_count = int(match.group(1).replace(' ', ''))
                                pages_count = math.ceil(product_count / 24)

                                tasks = []
                                for page_number in range(1, pages_count + 1):
                                    tasks.append(self.get_product_data(session, url,
                                                                       page_number))

                                res = await asyncio.gather(*tasks)
            except Exception as e:
                self.exception_tasks_categories.add(url)

    async def gather_all_urls(self):
        sem = asyncio.Semaphore(self.SEM_LIMIT)
        tasks = [self.gather_data(url, sem) for url in self.urls]
        res = await asyncio.gather(*tasks)

    async def repeat_requests_to_pages(self):
        sem = asyncio.Semaphore(self.SEM_LIMIT)
        count = 0
        while True:
            if len(self.exception_tasks) == 0:
                break
            if count == 10:
                break
            tasks = []
            try:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    for path in self.exception_tasks:
                        tasks.append(self.get_product_data(session, path[0], path[1]))
                    self.exception_tasks = set()
                    res = await asyncio.gather(*tasks)
            except Exception as e:
                print(f"[ERROR] Ошибка  {e}")
            count += 1
            print(f"[ITERATION] Счетчик  {count}")

    async def repeat_requests_to_categories(self):
        sem = asyncio.Semaphore(self.SEM_LIMIT)
        count = 0
        while True:
            if len(self.exception_tasks_categories) == 0:
                break
            if count == 10:
                break
            tasks = [self.gather_data(url, sem) for url in self.exception_tasks_categories]
            self.exception_tasks_categories = set()
            res = await asyncio.gather(*tasks)
            count += 1

    async def run(self):
        await self.gather_all_urls()
        await self.repeat_requests_to_pages()
        await self.repeat_requests_to_categories()

    def run_and_save_to_json(self):
        asyncio.run(self.run())
        with open('products.jsonl', 'w', encoding='utf-8') as jsonl_file:
            for product in self.products_data:
                jsonl_file.write(json.dumps(product, ensure_ascii=False) + '\n')


if __name__ == "__main__":
    urls = [
        'https://goldapple.ru/makijazh'
    ]

    scraper = ProductScraper(urls)
    scraper.run_and_save_to_json()
