import json
import utils
import settings
import time
from random import uniform, randint

if __name__ == '__main__':
    kafka_producer = utils.connect_kafka_producer()
    while True:
        optimums = [randint(11, 57) for _ in range(settings.N_MACHINES + 1)]
        for i, end in zip(range(settings.BATCH_SIZE), ([False] * (settings.BATCH_SIZE - 1)) + [True]):
            if i % 5 == 0:
                data = [uniform(-0.3, 1.1) for _ in range(settings.N_MACHINES + 1)]
                for idx, datum in enumerate(data):
                    good = int(datum * optimums[idx])
                    reject = optimums[idx] - good
                    if good < 0:
                        good = 0
                    if reject < 0 or reject > optimums[idx]:
                        reject = 0
                    data[idx] = (reject, good)
                # data = [(randint(-2, 8), randint(-3, 16)) for i in range(11)]
                # data = [(datum[0] if datum[0] > 0 else 0, datum[1] if datum[1] > 0 else 0) for datum in data]
            for j in range(11):
                payload = {
                    'end': end,
                    'reject': data[j][0],
                    'good': data[j][1],
                    'total': i * 2,
                    'optimum': optimums[j],
                    'id': f'{j}'
                }
                payload = json.dumps(payload)
                utils.publish_message(
                    kafka_producer,
                    settings.ACCUMULATION_TOPIC,
                    settings.KEY,
                    payload
                )

                time.sleep(0.27 / settings.N_MACHINES)