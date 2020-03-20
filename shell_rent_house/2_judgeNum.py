import asyncio
from urllib.parse import urljoin

import aiohttp
import requests
from lxml import etree

from shell_rent_house.redis_pool import redis_pool


class KeRentHouse():
    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'sec_url'

    @staticmethod
    def get_one_ip():
        try:
            ip_msg = requests.get(url='http://47.97.83.220/560527b49c4233ce38b3976a92ff58b5/proxy/xhs').json()['proxy']
            return ip_msg
        except Exception as e:
            return {'http': None, 'https': None}

    async def get_request(self, url):
        headers = {
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
            "referer": 'https://bj.zu.ke.com'
        }
        proxy = self.get_one_ip()
        proxies = proxy.get('http')
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url=url, headers=headers, proxy=proxies, timeout=3) as r:
                    response = await r.text()
                    state = r.status
                    return response, state
            except Exception as e:
                return 0, 0

    def response_area(self, response, url, city_logogram, city, area, data):
        tree = etree.HTML(response)
        num_text = tree.xpath('//span[@class="content__title--hl"]/text()')
        if num_text:
            num = num_text[0]
        else:
            num = 0
        if int(num) <= 3000:
            self.redis.lpush('third', data)
        else:
            # area = tree.xpath('//div[@id="filter"]/ul[4]/li/a/text()')
            area_url = tree.xpath('//div[@id="filter"]/ul[4]/li/a/@href')
            urls = [urljoin(url, i) for i in area_url]
            all_data = []
            for j in range(len(area_url[1:])):
                all_data.append('$$$'.join([urls[1:][j], city_logogram, city, area]))
            self.redis.lpush('sec_url', *all_data)

    async def start(self):
        data = self.redis.rpop(self.redis_key)
        if data:
            info = data.split('$$$')
            url = info[0]
            city_logogram = info[1]
            city = info[2]
            area = info[3]
            response, state = await self.get_request(url)
            if state == 200:
                print('请求成功---')
                self.response_area(response, url, city_logogram, city, area, data)
            else:
                self.redis.lpush(self.redis_key, data)
            return 1
        else:
            return 0

    def run(self):
        loop = asyncio.get_event_loop()
        while True:
            tasks = []
            for i in range(10):
                tasks.append(asyncio.ensure_future(self.start()))
            state = loop.run_until_complete(asyncio.gather(*tasks))
            if 1 not in state:
                print('redis已空----')
                break


house = KeRentHouse()
house.run()




