import asyncio
from urllib.parse import urljoin

import aiohttp
import requests
from lxml import etree

from shell_rent_house.redis_pool import redis_pool


class KeRentHouse():
    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'page_url'

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

    def response_area(self, response, url, city_logogram, city):
        tree = etree.HTML(response)
        divs = tree.xpath('//div[@class="content__list"]/div')
        for div in divs:
            urls = div.xpath('.//p[@class="content__list--item--title twoline"]/a/@href')
            title = div.xpath('.//p[@class="content__list--item--title twoline"]/a/text()')
            if title:
                price = div.xpath('.//span[@class="content__list--item-price"]/em/text()')
                community = div.xpath('.//p[@class="content__list--item--des"]/a[3]/text()')
                district = div.xpath('.//p[@class="content__list--item--des"]/a[1]/text()')
                sub_district = div.xpath('.//p[@class="content__list--item--des"]/a[2]/text()')
                titles = title[0].replace('\n', '').replace('\r', '').replace('\t', '').strip()
                if not price:
                    price = ['']
                if not community:
                    community = ['']
                if not district:
                    district = ['']
                if not sub_district:
                    sub_district = ['']
                base_url = urljoin(url, urls[0])
                all_data = '$$$'.join([base_url, city_logogram, city, titles, price[0], community[0], district[0], sub_district[0]])
                self.redis.lpush('list_info', all_data)
            else:
                print('title不存在，请检查url:::', url)

    async def start(self):
        data = self.redis.rpop(self.redis_key)
        # data = 'https://sh.zu.ke.com/zufang/zhangjiang/pg1rt200600000001//$$$sz$$$深圳$$$龙岗区'
        if data:
            info = data.split('$$$')
            url = info[0]
            city_logogram = info[1]
            city = info[2]
            area = info[3]
            response, state = await self.get_request(url)
            if state == 200:
                print('请求成功---')
                self.response_area(response, url, city_logogram, city)
            else:
                self.redis.lpush(self.redis_key, data)
            return 1
        else:
            return 0

    def run(self):
        loop = asyncio.get_event_loop()
        while True:
            tasks = []
            for i in range(15):
                tasks.append(asyncio.ensure_future(self.start()))
            state = loop.run_until_complete(asyncio.gather(*tasks))
            if 1 not in state:
                print('redis已空----')
                break


house = KeRentHouse()
house.run()




