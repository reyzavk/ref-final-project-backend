import msgpack
import faust
import json
import utils
import json
import settings


class State(faust.Record, serializer='json'):
    end: bool
    good: int
    reject: int
    total: int
    optimum: int
    runtime: int
    availability: float
    id: str


app = faust.App(settings.PERFORMANCE_TOPIC, broker='kafka://master.cluster2:9092')
app_topic = app.topic(settings.PERFORMANCE_TOPIC, value_type=State)
performance = app.Table(settings.PERFORMANCE_TABLE, default=lambda: 1.0)
kafka_producer = utils.connect_kafka_producer()

@app.agent(app_topic)
async def consume(states):
    with open('data/performance.msgpack', 'rb') as f:
        data = msgpack.unpackb(f.read())

    async for state in states.group_by(State.id):
        try:
            if not state.end:
                performance[state.id] -= 1 / settings.BATCH_SIZE * state.total / state.optimum
            else:
                performance[state.id] = 1

            payload = {
                'end': state.end,
                'good': state.good,
                'reject': state.reject,
                'total': state.total,
                'optimum': state.optimum,
                'runtime': state.runtime,
                'availability': state.availability,
                'performance': performance[state.id],
                'id': state.id,
            }
            with open('data/performance.msgpack', 'r+b') as f:
                data[state.id] = {
                   'performance': payload['performance'] 
                }
                f.write(msgpack.packb(data))
                f.truncate()

            payload = json.dumps(payload)
            utils.publish_message(kafka_producer, settings.QUALITY_TOPIC, settings.KEY, payload)
            print(f'performance id {state.id}: p -> {performance[state.id]}')
        except Exception as e:
            print(e)
