import asyncio
from urllib.parse import urljoin

import aiohttp
import requests
from lxml import etree

from shell_rent_house.redis_pool import redis_pool


class KeRentHouse():
    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'house'
        self.res = self.set_urls()

    def set_urls(self):
        self.redis.delete('start_url')
        urls = ['https://gz.zu.ke.com/zufang/rt200600000002/$$$gz$$$广州', 'https://gz.zu.ke.com/zufang/rt200600000001/$$$gz$$$广州',
                'https://sh.zu.ke.com/zufang/rt200600000002/$$$sh$$$上海', 'https://sh.zu.ke.com/zufang/rt200600000001/$$$sh$$$上海',
                'https://bj.zu.ke.com/zufang/rt200600000002/$$$bj$$$北京', 'https://bj.zu.ke.com/zufang/rt200600000001/$$$bj$$$北京',
                'https://sz.zu.ke.com/zufang/rt200600000002/$$$sz$$$深圳', 'https://sz.zu.ke.com/zufang/rt200600000001/$$$sz$$$深圳']
        res = self.redis.lpush('start_url', *urls)
        print('保存到redis成功----::::', res)

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

    def response_area(self, response, url):
        tree = etree.HTML(response)
        area = tree.xpath('//div[@id="filter"]/ul[2]/li/a/text()')
        area_url = tree.xpath('//div[@id="filter"]/ul[2]/li/a/@href')
        urls = [urljoin(url, i) for i in area_url]
        return area[1:], urls[1:]

    async def start(self):
        data = self.redis.rpop('start_url')
        if data:
            info = data.split('$$$')
            url = info[0]
            city_logogram = info[1]
            city = info[2]
            response, state = await self.get_request(url)
            if state == 200:
                all_data = []
                area, urls = self.response_area(response, url)
                for j in range(len(area)):
                    all_data.append('$$$'.join([urls[j], city_logogram, city, area[j]]))
                self.redis.lpush('sec_url', *all_data)
            else:
                self.redis.lpush('start_url', data)
            return 1
        else:
            return 0

    def run(self):
        loop = asyncio.get_event_loop()
        while True:
            tasks = []
            for i in range(4):
                tasks.append(asyncio.ensure_future(self.start()))
            state = loop.run_until_complete(asyncio.gather(*tasks))
            if 1 not in state:
                print('redis已空----')
                break


house = KeRentHouse()
house.run()




