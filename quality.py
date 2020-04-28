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
    performance: float
    id: str


app = faust.App(settings.QUALITY_TOPIC, broker='kafka://master.cluster2:9092')
app_topic = app.topic(settings.QUALITY_TOPIC, value_type=State)
quality = app.Table(settings.QUALITY_TABLE, default=lambda: 1.0)
kafka_producer = utils.connect_kafka_producer()

@app.agent(app_topic)
async def consume(states):
    with open('data/quality.msgpack', 'rb') as f:
        data = msgpack.unpackb(f.read())

    async for state in states.group_by(State.id):
        try:
            if not state.end:
                if state.total > 0:
                    quality[state.id] -= 1 / settings.BATCH_SIZE * state.reject / state.total
            else:
                quality[state.id] = 1

            payload = {
                'end': state.end,
                'good': state.good,
                'reject': state.reject,
                'total': state.total,
                'optimum': state.optimum,
                'runtime': state.runtime,
                'availability': state.availability,
                'performance': state.performance,
                'quality': quality[state.id],
                'id': state.id,
            }
            with open('data/quality.msgpack', 'wb') as f:
                data[state.id] = {
                   'quality': payload['quality'] 
                }
                f.write(msgpack.packb(data))
                f.truncate()

            payload = json.dumps(payload)
            utils.publish_message(kafka_producer, settings.OEE_TOPIC, settings.KEY, payload)
            print(f'quality id {state.id}: q -> {quality[state.id]}')
        except Exception as e:
            print(e)