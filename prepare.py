import msgpack

metrics = ['accumulation', 'availability', 'performance', 'quality', 'oee']
for metric in metrics:
    with open(f'data/{metric}.msgpack', 'wb') as f:
        f.write(msgpack.packb({}))