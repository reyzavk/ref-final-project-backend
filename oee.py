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
    quality: float
    id: str


app = faust.App(settings.OEE_TOPIC, broker='kafka://master.cluster2:9092')
app_topic = app.topic(settings.OEE_TOPIC, value_type=State)
oee = app.Table(settings.OEE_TABLE, default=lambda: 1.0)
kafka_producer = utils.connect_kafka_producer()

@app.agent(app_topic)
async def consume(states):
    with open('data/oee.msgpack', 'rb') as f:
        data = msgpack.unpackb(f.read())

    async for state in states.group_by(State.id):
        try:
            if not state.end:
                oee[state.id] = state.availability * state.performance * state.quality
            else:
                oee[state.id] = 1

            payload = {
                'end': state.end,
                'good': state.good,
                'reject': state.reject,
                'total': state.reject,
                'optimum': state.optimum,
                'runtime': state.runtime,
                'availability': state.availability,
                'performance': state.performance,
                'quality': state.quality,
                'oee': oee[state.id],
                'id': state.id,
            }
            with open('data/oee.msgpack', 'r+b') as f:
                data[state.id] = {
                    'oee': payload['oee']
                }
                f.write(msgpack.packb(data))
                f.truncate()

            payload = json.dumps(payload)
            print(f'oee id {state.id}: oee -> {oee[state.id]}')
        except Exception as e:
            print(e)
