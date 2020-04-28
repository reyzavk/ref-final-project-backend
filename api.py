import msgpack
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import settings

app = FastAPI()

origins = [
    'http://localhost:3000',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

@app.get("/sync/{metric}")
def sync(metric: str):
    with open(f'data/{metric}.msgpack', 'rb') as f:
        ret = []
        raw = f.read()
        data = msgpack.unpackb(raw)
        keys = sorted([key for key in data.keys()])
        for key in keys:
            datum = data[key]
            datum['id'] = int(key)
            if metric != 'accumulation':
                datum['good'] = int(datum[metric] * 100)
                datum['reject'] = 100 - datum['good']
            ret.append(datum)
        return ret