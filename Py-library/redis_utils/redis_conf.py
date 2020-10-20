# 开发环境
REDIS_CONFIG_DEV = {
    'redis_name': {
        'host': "127.0.0.1",
        'port': 6379,
        'password': '',
        'max_connections': 100,
        'db': 0
    },
}

# 线上环境
REDIS_CONFIG_PROD = {
    'redis_name': {
        'host': "127.0.0.1",
        'port': 6379,
        'password': 'password',
        'max_connections': 100,
        'db': 0
    },

}
