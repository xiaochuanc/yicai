import pandas

from shell_rent_house.redis_pool import redis_pool
from shell_rent_house.db_pool_postgresql import DB_POOL


class BiLiComment(object):
    localhost = {'host': 'pgm-bp16282ena3ilfm8mo.pg.rds.aliyuncs.com', 'port': 3432, 'user': 'tapas', 'password': 'Aj@W3qtMuaD9zYmBpBCt7vRp', 'db': 'postgres',
                 'charset': 'utf8'}

    def __init__(self):
        self.redis = redis_pool(2)
        self.redis_key = 'house_info_8'
        self.db = DB_POOL(self.localhost, 1, 5)

    def get_url(self):
        count = self.redis.llen(self.redis_key)
        with self.redis.pipeline(transaction=False) as p:
            for i in range(count):
                p.rpop(self.redis_key)
            datas = p.execute()
            self.redis.lpush('house_info_9', *datas)
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

            sql = 'INSERT INTO "public"."shell_house"("source_id", "origin_url", "title", "price", "city", "city_cn", "area", "room", "floor", "toward", "commnuity", "district", "sub_district") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict("source_id") do update set title=EXCLUDED.title;'
            res = self.db.sql_exe_many(sql, list(zip(source_id, origin_url, title, price, city, city_cn, area, room, floor, toward, community, district, sub_district)))
            if res:
                print('保存成功---')
                print(res)
            else:
                print('保存失败')
            # df = pandas.DataFrame({
            #     "source_id": source_id,
            #     "origin_url": origin_url,
            #     "title": title,
            #     "price": price,
            #     "city": city,
            #     "city_cn": city_cn,
            #     "area": area,
            #     "room": room,
            #     "floor": floor,
            #     "toward": toward,
            #     "community": community,
            #     "district": district,
            #     "sub_district": sub_district
            # })
            #
            # df.to_excel('beike_shell.xlsx')


bili = BiLiComment()
bili.get_url()


