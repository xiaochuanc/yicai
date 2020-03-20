import pandas

from bilitest.redis_pool import redis_pool


class BiLiComment(object):

    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'detail'

    def get_url(self):
        count = self.redis.scard(self.redis_key)
        with self.redis.pipeline(transaction=False) as p:
            for i in range(count):
                p.spop(self.redis_key)
            datas = p.execute()
            self.redis.lpush('house_info', *datas)
            source_id = []
            origin_url = []
            title = []
            price = []
            city = []
            city_cn = []
            area = []
            room = []
            floor = []
            toward = []
            community = []
            district = []
            sub_district = []
            for data in datas:
                info = data.split('$$$')
                source_id.append(info[0])
                origin_url.append(info[1])
                title.append(info[2])
                price.append(info[3])
                city.append(info[4])
                city_cn.append(info[5])
                area.append(info[6])
                room.append(info[7])
                floor.append(info[8])
                toward.append(info[9])
                community.append(info[10])
                district.append(info[11])
                sub_district.append(info[12])
            df = pandas.DataFrame({
                "source_id": source_id,
                "origin_url": origin_url,
                "title": title,
                "price": price,
                "city": city,
                "city_cn": city_cn,
                "area": area,
                "room": room,
                "floor": floor,
                "toward": toward,
                "community": community,
                "district": district,
                "sub_district": sub_district
            })

            df.to_excel('beike_shell.xlsx')


bili = BiLiComment()
bili.get_url()