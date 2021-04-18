from fastapi import APIRouter, status
import synonyms
from aqe.schemas.response import Response
import time

router = APIRouter()


@router.get("/nearby/", response_model=Response, name="Get similar words")
async def nearby(data: str, top_k: int = 10, threshold: float = 0.8, lang='zh') -> Response:
    s_time = time.time()
    words = data.split(',')
    result = {}
    if lang != 'zh':
        print("{} langauge is not supported yet".format(lang))
        return Response(terms=result, took=time.time() - s_time)

    for w in words:
        if w in result:
            continue
        ts, scores = synonyms.nearby(w, top_k)
        temp = [(t, float(s)) for t, s in zip(ts, scores) if s >= threshold and t != w]
        result[w] = temp

    return Response(terms=result, took=time.time() - s_time)
