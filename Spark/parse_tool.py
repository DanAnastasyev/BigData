import re
from datetime import datetime as dt

log_format = re.compile( 
    r"(?P<host>[\d\.]+)\s" 
    r"(?P<identity>\S*)\s" 
    r"(?P<user>\S*)\s"
    r"\[(?P<time>.*?)\]\s"
    r'"(?P<request>.*?)"\s'
    r"(?P<status>\d+)\s"
    r"(?P<bytes>\S*)\s"
    r'"(?P<referer>.*?)"\s'
    r'"(?P<user_agent>.*?)"\s*'
)

def parse_logs(line):
    match = log_format.match(line)
    if not match:
        return ("", "", "", "", "", "", "" ,"", "")

    request = match.group('request').split()
    return (
        match.group('host'),
        match.group('time').split()[0],
        request[0],
        request[1],
        match.group('status'),
        match.group('bytes'),
        match.group('referer'),
        match.group('user_agent'),
        dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour
    )

geoinfo_format = re.compile(r'\"(?P<ip>[^\"]+)\",\s+\"GeoIP Country Edition: \w+, (?P<country>[^\"]+)\"')
def parse_geoinfo(line):
    match = geoinfo_format.match(line)
    if not match:
        return ("", "")
    return (
        match.group('ip'),
        match.group('country'),
    )