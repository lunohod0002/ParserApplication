import math
import re
import aiohttp
import asyncio
import json
from bs4 import BeautifulSoup


class ProductScraper:

    def __init__(self, urls):
        self.urls = urls
        self.products_data = []
        self.SEM_LIMIT = 10  # Ограничение на количество одновременных работующих потоков

    async def get_product_data(self, session, url, page):
        """Получение данных о продуктах с указанной страницы."""
        try:
            full_url = f"{url}?p={page}"
            async with session.get(url=full_url) as response:
                soup = BeautifulSoup(await response.text(), "lxml")
                articles = soup.find_all('article')
                for article in articles:
                    # Извлекаем нужные данные о продукте
                    price_meta = article.find('meta', itemprop='price')
                    price = price_meta['content'] if price_meta else None

                    brand_meta = article.find('span', itemprop='brand').find('meta', itemprop='name')
                    brand_span = article.find('span', class_='Padcv')
                    brand = brand_meta['content'] if brand_meta else (
                        brand_span.get_text(strip=True) if brand_span else None)

                    name_span = article.find('span', class_='KkVNn')
                    product_name = name_span.get_text(strip=True) if name_span else None

                    availability_meta = article.find('meta', itemprop='availability')
                    in_stock = availability_meta[
                                   'content'] == 'https://schema.org/InStock' if availability_meta else False

                    sku_meta = article.find('meta', itemprop='sku')
                    product_id = sku_meta['content'] if sku_meta else None

                    product_type_div = article.find('div', class_='_7uTPQ')
                    product_type = product_type_div.get_text(strip=True) if product_type_div else None

                    images = article.find_all('source', type='image/jpeg')
                    image_urls = [img['srcset'] for img in images]

                    # Добавляение собранные данных в итоговый список
                    self.products_data.append({
                        "brand": brand,
                        "product_name": product_name,
                        "price": price,
                        "in_stock": in_stock,
                        "product_id": product_id,
                        "product_type": product_type,
                        "images": image_urls,
                    })
                print(f"[INFO] Обработал страницу {page} по {full_url}")
        except Exception as e:
            print(f"[ERROR] Ошибка на {url}?p={page}: {e}")

    async def gather_data(self, url, sem):
        """Собираем данные с одного URL, соблюдая лимит на количество потоков."""
        async with sem:  # Ограничивае количества одновременно работующих потоков
            try:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    async with session.get(url=url) as response:
                        html = await response.text()
                        soup = BeautifulSoup(html, "lxml")
                        # Поиск количества продуктов
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

                                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"[ERROR] Ошибка на {url}: {e}")

    async def gather_all_urls(self):
        """Запуск сбора данных для каждой URL"""
        sem = asyncio.Semaphore(self.SEM_LIMIT)  # Ограничение на количество потоков
        tasks = [self.gather_data(url, sem) for url in self.urls]
        await asyncio.gather(*tasks)

    def run_and_save_to_json(self):
        """Запуск процесса сбора данных."""
        asyncio.run(self.gather_all_urls())

        with open('products.jsonl', 'w', encoding='utf-8') as jsonl_file:
            for product in self.products_data:
                jsonl_file.write(json.dumps(product, ensure_ascii=False) + '\n')


if __name__ == "__main__":
    # Список URL для сбора данных
    urls = [
        'https://goldapple.ru/makijazh',
        'https://goldapple.ru/uhod',
        'https://goldapple.ru/volosy',
        'https://goldapple.ru/parfjumerija',
        'https://goldapple.ru/zdorov-e-i-apteka',
        'https://goldapple.ru/sexual-wellness',
        'https://goldapple.ru/azija',
        'https://goldapple.ru/organika',
        'https://goldapple.ru/dlja-muzhchin',
        'https://goldapple.ru/dlja-detej',
        'https://goldapple.ru/tehnika',
        'https://goldapple.ru/dlja-doma',
        'https://goldapple.ru/odezhda-i-aksessuary',
        'https://goldapple.ru/nizhnee-bel-jo',
        'https://goldapple.ru/ukrashenija',
        'https://goldapple.ru/mini-formaty',
        'https://goldapple.ru/tovary-dlja-zhivotnyh',
        'https://goldapple.ru/promo',
    ]

    scraper = ProductScraper(urls)  # Создаем экземпляр класса ProductScraper
    scraper.run()  # Запускаем сбор данных
