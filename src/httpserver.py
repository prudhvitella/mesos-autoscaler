from flask import Flask
app = Flask("autoscaler")


class HttpServer:
    @app.route("/")
    def hello():
        return "Hello World!"

    def start(self):
        app.run(host='0.0.0.0', port=self.port, debug=self.debug)

    def __init__(self, debug=False, listen_port=5000):
        self.debug = debug
        self.port = listen_port
