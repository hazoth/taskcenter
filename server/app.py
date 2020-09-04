import uvicorn
import time
from fastapi import FastAPI
# from fastapi import Request
# from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, PlainTextResponse
from . import app_task


app = FastAPI()
app.include_router(app_task.router, prefix='/task')


@app.on_event('startup')
async def app_startup():
    from concurrent.futures import ThreadPoolExecutor
    import asyncio
    tpe = ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(tpe)
    await app_task.get_task_manager()


# @app.middleware('http')
# async def app_logger(request: Request, call_next):
#     t0 = time.time()
#     response = await call_next(request)
#     logger.info('[http] %s %s %s %.4f'%(request.method, request.url, 
#         response.status_code, time.time() - t0))
#     return response


# @app.exception_handler(AppException)
# async def error_code_handler(request: Request, exc: AppException):
#     content = {"error_code": exc.error_code}
#     if exc.error_msg is not None:
#         content['error_msg'] = exc.error_msg
#     return JSONResponse(status_code=200, content=content)


@app.get('/version', response_class=PlainTextResponse)
async def version():
    return FileResponse('version')


@app.get('/buildtime', response_class=PlainTextResponse)
async def buildtime():
    return FileResponse('buildtime')


if __name__ == "__main__":
    import uvicorn
    # from fastapi.middleware.cors import CORSMiddleware
    # app.add_middleware(
    #     CORSMiddleware,
    #     allow_origins=['*'],
    #     allow_credentials=True,
    #     allow_methods=["*"],
    #     allow_headers=["*"],
    # )
    uvicorn.run(app, host="0.0.0.0", port=8000)
