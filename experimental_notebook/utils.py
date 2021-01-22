import datetime
import tzlocal
import pytz

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Taken from https://chromium.googlesource.com/infra/infra/infra_libs/+/b2e2c9948c327b88b138d8bd60ec4bb3a957be78/time_functions/testing.py
class MockDateTimeMeta(datetime.datetime.__dict__.get('__metaclass__', type)):
    @classmethod
    def __instancecheck__(cls, instance):
      return isinstance(instance, datetime.datetime)

# Taken from https://chromium.googlesource.com/infra/infra/infra_libs/+/b2e2c9948c327b88b138d8bd60ec4bb3a957be78/time_functions/testing.py
class MockDateTime():
    __metaclass__ = MockDateTimeMeta
    mock_utcnow = datetime.datetime.utcnow()
    tzinfo = "mock"

    @classmethod
    def isoformat(cls):
        return cls.mock_utcnow.strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def utcnow(cls):
        return cls.mock_utcnow

    @classmethod
    def now(cls, tz=None):
        if not tz:
            tz = tzlocal.get_localzone()
        tzaware_utcnow = pytz.utc.localize(cls.mock_utcnow)
        return tz.normalize(tzaware_utcnow.astimezone(tz)).replace(tzinfo=None)

    @classmethod
    def today(cls):
        return cls.now().date()

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None):
        if not tz:
            # TODO(sergiyb): This may fail for some unclear reason because pytz
            # doesn't find normal timezones such as 'Europe/Berlin'. This seems to
            # happen only in appengine/chromium_try_flakes tests, and not in tests
            # for this module itself.
            tz = tzlocal.get_localzone()
        tzaware_dt = pytz.utc.localize(cls.utcfromtimestamp(timestamp))
        return tz.normalize(tzaware_dt.astimezone(tz)).replace(tzinfo=None)