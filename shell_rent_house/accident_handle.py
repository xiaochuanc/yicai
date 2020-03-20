import asyncio
from urllib.parse import urljoin

import aiohttp
import requests
from lxml import etree

from shell_rent_house.redis_pool import redis_pool
from shell_rent_house.coordtransform import bd09_to_wgs84


class KeRentHouse():
    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'list_info'

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

    def response_area(self, response):
        tree = etree.HTML(response)
        longitude_latitude = tree.xpath('//div[@class="map__cur"]/@data-coord')
        if longitude_latitude:
            longitude = longitude_latitude[0].get('longitude', '')
            latitude = longitude_latitude[0].get('latitude', '')
        else:
            longitude = ''
            latitude = ''
        station = tree.xpath('//ul[@class="content__map--overlay__list"]/li[1]/p[1]/text()')
        distance = tree.xapth('//ul[@class="content__map--overlay__list"]/li[1]/p[1]/span/text()')
        circuit_subway = tree.xpath('//ul[@class="content__map--overlay__list"]/li[1]/p[2]/text()')
        if not station:
            station = ['']
        if not distance:
            distance = ['']
        if not circuit_subway:
            circuit_subway = ['']

        metro = station + '-' + distance + '-' + circuit_subway

        return longitude, latitude, metro

    async def start(self):
        data = self.redis.rpop(self.redis_key)
        # data = 'https://sz.zu.ke.com/zufang/SZ2368643339651391488.html$$$sz$$$深圳$$$整租·英郡年华 1室1厅 西南$$$2500$$$英郡年华$$$龙岗区$$$丹竹头'
        if data:
            info = data.split('$$$')
            source_id = info[0]
            url = info[1]
            title = info[2]
            price = info[3]
            city = info[4]
            city_logogram = info[5]
            area = info[6]
            room = info[7]
            floor = info[8]
            toward = info[9]
            community = info[10]
            district = info[11]
            sub_district = info[12]
            response, state = await self.get_request(url)
            if state == 200:
                print('请求成功---')
                longitude, latitude, metro = self.response_area(response)
                wgs_longitude_latitude = bd09_to_wgs84(longitude, latitude)
                all_data = '$$$'.join([source_id, url, title, price, city, city_logogram, area, room, floor, toward, community, district, sub_district, longitude, latitude, wgs_longitude_latitude[0], wgs_longitude_latitude[1], metro])
                self.redis.sadd('detail', all_data)
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




