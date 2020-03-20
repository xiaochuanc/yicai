import redis


def redis_pool(ku):
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True, db=ku)
    red = redis.Redis(connection_pool=pool)

    return red



