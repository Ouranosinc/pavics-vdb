from pydantic import BaseModel
from enum import Enum


def test_regexp_for_ensemble_member():
    class TestMem(BaseModel):
        member: cv.cmip5_member

    TestMem(member='r1i2p3')



class Test(BaseModel):
    e: Enum("E", {"A": "a"})
