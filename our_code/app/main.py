from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1 import auth, booking_results, bookings, callslots, feedbacks, houses, statistics
from app.services import examples_service


BASE_DIR = Path(__file__).resolve().parent
API_STATUS_PAGE = BASE_DIR / "static" / "api_status.html"


class CompactJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        encoded = jsonable_encoder(content, exclude_none=True)
        return super().render(encoded)


def create_app() -> FastAPI:
    app = FastAPI(
        title="HouseUp API",
        description="API for the HouseUp real-estate platform.",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        default_response_class=CompactJSONResponse,
    )

    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(houses.router, prefix="/api/v1")
    app.include_router(bookings.router, prefix="/api/v1")
    app.include_router(callslots.router, prefix="/api/v1")
    app.include_router(feedbacks.router, prefix="/api/v1")
    app.include_router(booking_results.router, prefix="/api/v1")
    app.include_router(statistics.router, prefix="/api/v1")

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 405:
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "detail": (
                        "HTTP method not allowed for this endpoint. "
                        "Check the API reference or documentation for the correct method."
                    ),
                    "method": request.method,
                    "path": request.url.path,
                },
            )
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.get("/", include_in_schema=False)
    async def root():
        if API_STATUS_PAGE.exists():
            return FileResponse(API_STATUS_PAGE)
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "service": "HouseUp API",
                "docs": "/docs",
                "openapi": "/openapi.json",
            },
        )

    @app.get("/examples/live", include_in_schema=False, response_class=HTMLResponse)
    async def examples_live():
        snapshot = examples_service.get_examples_snapshot()
        return HTMLResponse(content=examples_service.render_examples_html(snapshot))

    @app.get("/api/v1/examples/snapshot", include_in_schema=False)
    async def examples_snapshot():
        return examples_service.get_examples_snapshot()

    return app


app = create_app()
