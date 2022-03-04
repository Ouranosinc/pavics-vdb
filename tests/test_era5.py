from base import TestNcML


class TestERA5(TestNcML):
    path = 'datasets/reanalyses/era5.ncml'
    has_variables = ['pr', 'tas']
    has_date = ['2001-01-01', '2018-12-31']


def test_era5():
    t = TestERA5
    t.test()
