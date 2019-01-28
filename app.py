from flask import Flask
from flask import request
import json
from pipeline.graph import Graph
from flask_cors import CORS
from flask import make_response
from pipeline.status import Status
from pipeline.sys_tables import GraphTable, StepTable

app = Flask(__name__)
CORS(app, supports_credentials=True)


@app.route("/")
def index():
    return "Hello world"


@app.after_request
def after_request(resp):
    resp = make_response(resp)
    resp.headers['Access-Control-Allow-Credentials'] = 'true'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-Requested-With'
    return resp


@app.route("/run", methods=["POST"])
def run():
    config = json.loads(request.data)
    print(config)

    global graph
    graph = Graph(config)
    graph.start(run_or_show="run")
    global cur_graph_id, cur_graph_status
    cur_graph_id = graph.id
    cur_graph_status = graph.status
    if cur_graph_status == Status.finished:
        return str(Status.finished)
    else:
        return str(Status.failed)


@app.route("/get_data", methods=["GET"])
def get_data():
    try:
        graph
    except:
        from pipeline.connector_factory import ConnectorFactory
        connector = ConnectorFactory('mysql')
        with connector.init_session() as sess:
            max_graph = sess.query(GraphTable).order_by(GraphTable.id.desc()).first()
            latest_graph_id, latest_graph_status = (getattr(max_graph, "id"), getattr(max_graph, "status"))
            # step = sess.query(StepTable).filter_by(StepTable.graph_id == graph.id)
        return json.dumps({
            "graph_status": latest_graph_status,
            "steps_status": [],
            "data": []
        })

    try:
        sample_data = graph.sample(10)
        result = {
            "graph_status": cur_graph_status,
            "steps_status": graph.steps_status,
            "data": sample_data
        }
        return json.dumps(result)
    except:
        return json.dumps({
            "graph_status": Status.failed,
            "steps_status": [],
            "data": []
        })


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=False)

