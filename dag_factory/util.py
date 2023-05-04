class RetryTimes():
    VeryLow = 0
    Low = 1
    High = 10
    VeryHigh = 25

    def get_retry_times(self, times:str) -> int:
        if times == 'VeryLow':
            return self.VeryLow
        if times == 'High':
            return self.High
        if times == 'VeryHigh':
            return self.VeryHigh
        return self.Low

class DelayTime():
    Short = 1
    Long = 15
    VeryLong = 30

    def get_delay_time(self, wait:str) -> int:
        if wait == 'Long':
            return self.Long
        if wait == 'VeryLong':
            return self.VeryLong
        return self.Short


class SensorWaitTime():
    Short = 2
    Long = 15
    VeryLong = 30

    def get_wait_time(self, wait:str) -> int:
        if wait == 'Long':
            return self.Long
        if wait == 'VeryLong':
            return self.VeryLong
        return self.Short