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
    id: str


app = faust.App(settings.ACCUMULATION_TOPIC, broker=settings.BROKER)
app_topic = app.topic(settings.ACCUMULATION_TOPIC, value_type=State)
good_accumulated = app.Table(settings.GOOD_ACCUMULATED_TABLE, default=int)
reject_accumulated = app.Table(settings.REJECT_ACCUMULATED_TABLE, default=int)
kafka_producer = utils.connect_kafka_producer()

@app.agent(app_topic)
async def consume(states):
    with open('data/accumulation.msgpack', 'rb') as f:
        data = msgpack.unpackb(f.read())

    async for state in states.group_by(State.id):
        try:
            if not state.end:
                good_accumulated[state.id] += state.good
                reject_accumulated[state.id] += state.reject
            else:
                good_accumulated[state.id] = 0
                reject_accumulated[state.id] = 0

            payload = {
                'end': state.end,
                'good': state.good,
                'reject': state.reject,
                'total': state.reject + state.good,
                'optimum': state.optimum,
                'id': state.id,
            }
            with open('data/accumulation.msgpack', 'wb') as f:
                data[state.id] = {
                    'good': good_accumulated[state.id],
                    'reject': reject_accumulated[state.id],
                }
                f.write(msgpack.packb(data))
                f.truncate()

            payload = json.dumps(payload)
            utils.publish_message(kafka_producer, settings.AVAILABILITY_TOPIC, settings.KEY, payload)
            print(f'accumulation id {state.id} : g -> {good_accumulated[state.id]} r -> {reject_accumulated[state.id]}')
        except Exception as e:
            print(e)