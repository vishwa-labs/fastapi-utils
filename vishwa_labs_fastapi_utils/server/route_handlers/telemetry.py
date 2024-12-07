from fastapi import APIRouter


class TelemetryAPI:
    def __init__(self) -> None:
        self.router = APIRouter()
        self.router.add_api_route("/healthcheck", self.healthcheck_endpoint, methods=["GET"])
        self.router.add_api_route("/ping", self.healthcheck_endpoint, methods=["GET"])

    async def healthcheck_endpoint(self):
        return "!pong"
