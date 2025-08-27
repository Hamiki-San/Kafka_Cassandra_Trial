from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, HTMLResponse
from cassandra.cluster import Cluster
from uuid import uuid5, NAMESPACE_URL

KEYSPACE = "mimic_data"
CASSANDRA_HOSTS = ["127.0.0.1"]  # change if needed

app = FastAPI()
cluster = Cluster(CASSANDRA_HOSTS)
session = cluster.connect(KEYSPACE)

SEL_LIST = session.prepare("SELECT key, timestamp, output_data, timestamp FROM data_sensor1")


def file_id_from_filename(name: str):
    return uuid5(NAMESPACE_URL, name)

@app.get("/", response_class=HTMLResponse)
def index():
    rows = session.execute(SEL_LIST, ())
    links = []
    for r in rows:
        fname = r.filename
        links.append(f'<li><a href="/files/{fname}">{fname}</a> '
                     f'({r.size} bytes, {r.chunks} chunks)</li>')
    html = "<h2>Stored Files</h2><ul>" + "\n".join(links) + "</ul>"
    return html

@app.get("/files/{filename}")
def get_file(filename: str):
    fid = file_id_from_filename(filename)
    rows = session.execute(SEL_CHUNKS, (fid,))
    content = b"".join(bytes(r.data) for r in rows)
    if not content:
        raise HTTPException(status_code=404, detail="File not found")
    # naive MIME—good enough for demo
    low = filename.lower()
    if low.endswith(".jpg") or low.endswith(".jpeg"):
        mime = "image/jpeg"
    elif low.endswith(".png"):
        mime = "image/png"
    elif low.endswith(".pcd"):
        mime = "application/octet-stream"  # browser will download
    else:
        mime = "application/octet-stream"
    return Response(content=content, media_type=mime)
