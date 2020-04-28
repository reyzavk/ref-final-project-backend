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


app = faust.App(settings.AVAILABILITY_TOPIC, broker='kafka://master.cluster2:9092')
app_topic = app.topic(settings.AVAILABILITY_TOPIC, value_type=State)
n_downtime = app.Table(settings.N_DOWNTIME_TABLE, default=int)
availability = app.Table(settings.AVAILABILITY_TABLE, default=lambda: 1.0)
runtime = app.Table(settings.RUNTIME_TABLE, default=lambda: settings.TOTAL_TIME)
kafka_producer = utils.connect_kafka_producer()

@app.agent(app_topic)
async def consume(states):
    with open('data/availability.msgpack', 'rb') as f:
        data = msgpack.unpackb(f.read())

    async for state in states.group_by(State.id):
        try:
            if not state.end:
                if not state.good and not state.reject:
                    n_downtime[state.id] += 1
                    availability[state.id] -= 1 / settings.BATCH_SIZE
                    runtime[state.id] -= int(1 / settings.BATCH_SIZE * settings.TOTAL_TIME)
            else:
                n_downtime[state.id] = 0
                availability[state.id] = 1.0
                runtime[state.id] = settings.TOTAL_TIME

            payload = {
                'end': state.end,
                'good': state.good,
                'reject': state.reject,
                'total': state.total,
                'optimum': state.optimum,
                'runtime': runtime[state.id],
                'availability': availability[state.id],
                'id': state.id,
            }
            with open('data/availability.msgpack', 'wb') as f:
                data[state.id] = {
                    'availability': payload['availability'],
                    'runtime': payload['runtime']
                }
                f.write(msgpack.packb(data))
                f.truncate()

            payload = json.dumps(payload)
            utils.publish_message(kafka_producer, settings.PERFORMANCE_TOPIC, settings.KEY, payload)
            print(f'availability id {state.id}: a -> {availability[state.id]} runtime -> {runtime[state.id]}')
        except Exception as e:
            print(e)