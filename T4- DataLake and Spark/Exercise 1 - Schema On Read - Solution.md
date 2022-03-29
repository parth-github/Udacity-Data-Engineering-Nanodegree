
# Exercise 1: Schema on Read


```python
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
```


```python
spark = SparkSession.builder.getOrCreate()
```


```python
dfLog = spark.read.text("data/NASA_access_log_Jul95.gz")
```

# Load the dataset


```python
#Data Source: http://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
dfLog = spark.read.text("data/NASA_access_log_Jul95.gz")
```

# Quick inspection of  the data set


```python
# see the schema
dfLog.printSchema()
```

    root
     |-- value: string (nullable = true)
    



```python
# number of lines
dfLog.count()
```




    1891715




```python
#what's in there? 
dfLog.show(5)
```

    +--------------------+
    |               value|
    +--------------------+
    |199.72.81.55 - - ...|
    |unicomp6.unicomp....|
    |199.120.110.21 - ...|
    |burger.letters.co...|
    |199.120.110.21 - ...|
    +--------------------+
    only showing top 5 rows
    



```python
#a better show?
dfLog.show(5, truncate=False)
```

    +-----------------------------------------------------------------------------------------------------------------------+
    |value                                                                                                                  |
    +-----------------------------------------------------------------------------------------------------------------------+
    |199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245                                 |
    |unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985                      |
    |199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085   |
    |burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0               |
    |199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179|
    +-----------------------------------------------------------------------------------------------------------------------+
    only showing top 5 rows
    



```python
#pandas to the rescue
pd.set_option('max_colwidth', 200)
dfLog.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179</td>
    </tr>
  </tbody>
</table>
</div>



# Let' try simple parsing with split


```python
from pyspark.sql.functions import split
dfArrays = dfLog.withColumn("tokenized", split("value"," "))
dfArrays.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
      <th>tokenized</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245</td>
      <td>[199.72.81.55, -, -, [01/Jul/1995:00:00:01, -0400], "GET, /history/apollo/, HTTP/1.0", 200, 6245]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>[unicomp6.unicomp.net, -, -, [01/Jul/1995:00:00:06, -0400], "GET, /shuttle/countdown/, HTTP/1.0", 200, 3985]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085</td>
      <td>[199.120.110.21, -, -, [01/Jul/1995:00:00:09, -0400], "GET, /shuttle/missions/sts-73/mission-sts-73.html, HTTP/1.0", 200, 4085]</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0</td>
      <td>[burger.letters.com, -, -, [01/Jul/1995:00:00:11, -0400], "GET, /shuttle/countdown/liftoff.html, HTTP/1.0", 304, 0]</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179</td>
      <td>[199.120.110.21, -, -, [01/Jul/1995:00:00:11, -0400], "GET, /shuttle/missions/sts-73/sts-73-patch-small.gif, HTTP/1.0", 200, 4179]</td>
    </tr>
    <tr>
      <th>5</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0</td>
      <td>[burger.letters.com, -, -, [01/Jul/1995:00:00:12, -0400], "GET, /images/NASA-logosmall.gif, HTTP/1.0", 304, 0]</td>
    </tr>
    <tr>
      <th>6</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0</td>
      <td>[burger.letters.com, -, -, [01/Jul/1995:00:00:12, -0400], "GET, /shuttle/countdown/video/livevideo.gif, HTTP/1.0", 200, 0]</td>
    </tr>
    <tr>
      <th>7</th>
      <td>205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985</td>
      <td>[205.212.115.106, -, -, [01/Jul/1995:00:00:12, -0400], "GET, /shuttle/countdown/countdown.html, HTTP/1.0", 200, 3985]</td>
    </tr>
    <tr>
      <th>8</th>
      <td>d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>[d104.aa.net, -, -, [01/Jul/1995:00:00:13, -0400], "GET, /shuttle/countdown/, HTTP/1.0", 200, 3985]</td>
    </tr>
    <tr>
      <th>9</th>
      <td>129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074</td>
      <td>[129.94.144.152, -, -, [01/Jul/1995:00:00:13, -0400], "GET, /, HTTP/1.0", 200, 7074]</td>
    </tr>
  </tbody>
</table>
</div>



# Second attempt, let's build a custom parsing UDF 


```python
from pyspark.sql.functions import udf

@udf
def parseUDF(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN, line)
    if match is None:
        return (line, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = match.group(9)
    return {
        "host"          : match.group(1), 
        "client_identd" : match.group(2), 
        "user_id"       : match.group(3), 
        "date_time"     : match.group(4), 
        "method"        : match.group(5),
        "endpoint"      : match.group(6),
        "protocol"      : match.group(7),
        "response_code" : int(match.group(8)),
        "content_size"  : size
    }
```


```python
#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDF("value"))
dfParsed.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
      <th>parsed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/history/apollo/, content_size=6245, method=GET, date_time=01/Jul/1995:00:00:01 -0400, user_id=-, host=199.72.81.55, client_identd=-}</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/countdown/, content_size=3985, method=GET, date_time=01/Jul/1995:00:00:06 -0400, user_id=-, host=unicomp6.unicomp.net, client_identd=-}</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/missions/sts-73/mission-sts-73.html, content_size=4085, method=GET, date_time=01/Jul/1995:00:00:09 -0400, user_id=-, host=199.120.110.21, c...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0</td>
      <td>{response_code=304, protocol=HTTP/1.0, endpoint=/shuttle/countdown/liftoff.html, content_size=0, method=GET, date_time=01/Jul/1995:00:00:11 -0400, user_id=-, host=burger.letters.com, client_identd=-}</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/missions/sts-73/sts-73-patch-small.gif, content_size=4179, method=GET, date_time=01/Jul/1995:00:00:11 -0400, user_id=-, host=199.120.110.21...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0</td>
      <td>{response_code=304, protocol=HTTP/1.0, endpoint=/images/NASA-logosmall.gif, content_size=0, method=GET, date_time=01/Jul/1995:00:00:12 -0400, user_id=-, host=burger.letters.com, client_identd=-}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/countdown/video/livevideo.gif, content_size=0, method=GET, date_time=01/Jul/1995:00:00:12 -0400, user_id=-, host=burger.letters.com, client...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/countdown/countdown.html, content_size=3985, method=GET, date_time=01/Jul/1995:00:00:12 -0400, user_id=-, host=205.212.115.106, client_iden...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/shuttle/countdown/, content_size=3985, method=GET, date_time=01/Jul/1995:00:00:13 -0400, user_id=-, host=d104.aa.net, client_identd=-}</td>
    </tr>
    <tr>
      <th>9</th>
      <td>129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074</td>
      <td>{response_code=200, protocol=HTTP/1.0, endpoint=/, content_size=7074, method=GET, date_time=01/Jul/1995:00:00:13 -0400, user_id=-, host=129.94.144.152, client_identd=-}</td>
    </tr>
  </tbody>
</table>
</div>




```python
dfParsed.printSchema()
```

    root
     |-- value: string (nullable = true)
     |-- parsed: string (nullable = true)
    


# Third attempt, let's fix our UDF


```python
#from pyspark.sql.functions import udf # already imported
from pyspark.sql.types import MapType, StringType

@udf(MapType(StringType(),StringType()))
def parseUDFbetter(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN, line)
    if match is None:
        return (line, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = match.group(9)
    return {
        "host"          : match.group(1), 
        "client_identd" : match.group(2), 
        "user_id"       : match.group(3), 
        "date_time"     : match.group(4), 
        "method"        : match.group(5),
        "endpoint"      : match.group(6),
        "protocol"      : match.group(7),
        "response_code" : int(match.group(8)),
        "content_size"  : size
    }
```


```python
#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDFbetter("value"))
dfParsed.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
      <th>parsed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/history/apollo/', 'content_size': '6245', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:01 -0400', 'user_id': '-', 'host': '199.72...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:06 -0400', 'user_id': '-', 'host': 'uni...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/mission-sts-73.html', 'content_size': '4085', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:09 -0400', 'us...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0</td>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/liftoff.html', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', 'user_id': '-', 'ho...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'content_size': '4179', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', ...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0</td>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/images/NASA-logosmall.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-', 'host': ...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/video/livevideo.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/countdown.html', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-'...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': 'd10...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/', 'content_size': '7074', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': '129.94.144.152', 'cli...</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDFbetter("value"))
dfParsed.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
      <th>parsed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/history/apollo/', 'content_size': '6245', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:01 -0400', 'user_id': '-', 'host': '199.72...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:06 -0400', 'user_id': '-', 'host': 'uni...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/mission-sts-73.html', 'content_size': '4085', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:09 -0400', 'us...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0</td>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/liftoff.html', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', 'user_id': '-', 'ho...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'content_size': '4179', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', ...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0</td>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/images/NASA-logosmall.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-', 'host': ...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/video/livevideo.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/countdown.html', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-'...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': 'd10...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074</td>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/', 'content_size': '7074', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': '129.94.144.152', 'cli...</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Bingo!! we'got a column of type map with the fields parsed
dfParsed.printSchema()
```

    root
     |-- value: string (nullable = true)
     |-- parsed: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)
    



```python
dfParsed.select("parsed").limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>parsed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/history/apollo/', 'content_size': '6245', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:01 -0400', 'user_id': '-', 'host': '199.72...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:06 -0400', 'user_id': '-', 'host': 'uni...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/mission-sts-73.html', 'content_size': '4085', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:09 -0400', 'us...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/liftoff.html', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', 'user_id': '-', 'ho...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'content_size': '4179', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:11 -0400', ...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>{'response_code': '304', 'protocol': 'HTTP/1.0', 'endpoint': '/images/NASA-logosmall.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-', 'host': ...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/video/livevideo.gif', 'content_size': '0', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/countdown.html', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:12 -0400', 'user_id': '-'...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/shuttle/countdown/', 'content_size': '3985', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': 'd10...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>{'response_code': '200', 'protocol': 'HTTP/1.0', 'endpoint': '/', 'content_size': '7074', 'method': 'GET', 'date_time': '01/Jul/1995:00:00:13 -0400', 'user_id': '-', 'host': '129.94.144.152', 'cli...</td>
    </tr>
  </tbody>
</table>
</div>



# Let's build separate columns


```python
dfParsed.selectExpr("parsed['host'] as host").limit(5).show(5)
```

    +--------------------+
    |                host|
    +--------------------+
    |        199.72.81.55|
    |unicomp6.unicomp.net|
    |      199.120.110.21|
    |  burger.letters.com|
    |      199.120.110.21|
    +--------------------+
    



```python
dfParsed.selectExpr(["parsed['host']", "parsed['date_time']"]).show(5)
```

    +--------------------+--------------------+
    |        parsed[host]|   parsed[date_time]|
    +--------------------+--------------------+
    |        199.72.81.55|01/Jul/1995:00:00...|
    |unicomp6.unicomp.net|01/Jul/1995:00:00...|
    |      199.120.110.21|01/Jul/1995:00:00...|
    |  burger.letters.com|01/Jul/1995:00:00...|
    |      199.120.110.21|01/Jul/1995:00:00...|
    +--------------------+--------------------+
    only showing top 5 rows
    



```python
fields = ["host", "client_identd","user_id", "date_time", "method", "endpoint", "protocol", "response_code", "content_size"]
exprs = [ "parsed['{}'] as {}".format(field,field) for field in fields]
exprs

```




    ["parsed['host'] as host",
     "parsed['client_identd'] as client_identd",
     "parsed['user_id'] as user_id",
     "parsed['date_time'] as date_time",
     "parsed['method'] as method",
     "parsed['endpoint'] as endpoint",
     "parsed['protocol'] as protocol",
     "parsed['response_code'] as response_code",
     "parsed['content_size'] as content_size"]




```python
dfClean = dfParsed.selectExpr(*exprs)
dfClean.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>host</th>
      <th>client_identd</th>
      <th>user_id</th>
      <th>date_time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response_code</th>
      <th>content_size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:01 -0400</td>
      <td>GET</td>
      <td>/history/apollo/</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>6245</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:06 -0400</td>
      <td>GET</td>
      <td>/shuttle/countdown/</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>3985</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:09 -0400</td>
      <td>GET</td>
      <td>/shuttle/missions/sts-73/mission-sts-73.html</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>4085</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:11 -0400</td>
      <td>GET</td>
      <td>/shuttle/countdown/liftoff.html</td>
      <td>HTTP/1.0</td>
      <td>304</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:11 -0400</td>
      <td>GET</td>
      <td>/shuttle/missions/sts-73/sts-73-patch-small.gif</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>4179</td>
    </tr>
  </tbody>
</table>
</div>



## Popular hosts


```python
from pyspark.sql.functions import desc
dfClean.groupBy("host").count().orderBy(desc("count")).limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>host</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>piweba3y.prodigy.com</td>
      <td>17572</td>
    </tr>
    <tr>
      <th>1</th>
      <td>piweba4y.prodigy.com</td>
      <td>11591</td>
    </tr>
    <tr>
      <th>2</th>
      <td>piweba1y.prodigy.com</td>
      <td>9868</td>
    </tr>
    <tr>
      <th>3</th>
      <td>alyssa.prodigy.com</td>
      <td>7852</td>
    </tr>
    <tr>
      <th>4</th>
      <td>siltb10.orl.mmc.com</td>
      <td>7573</td>
    </tr>
    <tr>
      <th>5</th>
      <td>piweba2y.prodigy.com</td>
      <td>5922</td>
    </tr>
    <tr>
      <th>6</th>
      <td>edams.ksc.nasa.gov</td>
      <td>5434</td>
    </tr>
    <tr>
      <th>7</th>
      <td>163.206.89.4</td>
      <td>4906</td>
    </tr>
    <tr>
      <th>8</th>
      <td>news.ti.com</td>
      <td>4863</td>
    </tr>
    <tr>
      <th>9</th>
      <td>disarray.demon.co.uk</td>
      <td>4353</td>
    </tr>
  </tbody>
</table>
</div>



## Popular content


```python
from pyspark.sql.functions import desc
dfClean.groupBy("endpoint").count().orderBy(desc("count")).limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>endpoint</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/images/NASA-logosmall.gif</td>
      <td>111330</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/images/KSC-logosmall.gif</td>
      <td>89638</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/images/MOSAIC-logosmall.gif</td>
      <td>60467</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/images/USA-logosmall.gif</td>
      <td>60013</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/images/WORLD-logosmall.gif</td>
      <td>59488</td>
    </tr>
    <tr>
      <th>5</th>
      <td>/images/ksclogo-medium.gif</td>
      <td>58801</td>
    </tr>
    <tr>
      <th>6</th>
      <td>/images/launch-logo.gif</td>
      <td>40871</td>
    </tr>
    <tr>
      <th>7</th>
      <td>/shuttle/countdown/</td>
      <td>40278</td>
    </tr>
    <tr>
      <th>8</th>
      <td>/ksc.html</td>
      <td>40226</td>
    </tr>
    <tr>
      <th>9</th>
      <td>/images/ksclogosmall.gif</td>
      <td>33585</td>
    </tr>
  </tbody>
</table>
</div>



## Large Files


```python
dfClean.createOrReplaceTempView("cleanlog")
spark.sql("""
select endpoint, content_size
from cleanlog 
order by content_size desc
""").limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>endpoint</th>
      <th>content_size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/images/cdrom-1-95/img0007.jpg</td>
      <td>99981</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/shuttle/missions/sts-71/movies/sts-71-launch.mpg</td>
      <td>999424</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/shuttle/missions/sts-71/movies/sts-71-launch.mpg</td>
      <td>999424</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>5</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>6</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>7</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>8</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
    <tr>
      <th>9</th>
      <td>/history/apollo/apollo-13/images/index.gif</td>
      <td>99942</td>
    </tr>
  </tbody>
</table>
</div>




```python
from pyspark.sql.functions import expr
dfCleanTyped = dfClean.withColumn("content_size_bytes", expr("cast(content_size  as int)"))
dfCleanTyped.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>host</th>
      <th>client_identd</th>
      <th>user_id</th>
      <th>date_time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response_code</th>
      <th>content_size</th>
      <th>content_size_bytes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>199.72.81.55</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:01 -0400</td>
      <td>GET</td>
      <td>/history/apollo/</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>6245</td>
      <td>6245</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unicomp6.unicomp.net</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:06 -0400</td>
      <td>GET</td>
      <td>/shuttle/countdown/</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>3985</td>
      <td>3985</td>
    </tr>
    <tr>
      <th>2</th>
      <td>199.120.110.21</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:09 -0400</td>
      <td>GET</td>
      <td>/shuttle/missions/sts-73/mission-sts-73.html</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>4085</td>
      <td>4085</td>
    </tr>
    <tr>
      <th>3</th>
      <td>burger.letters.com</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:11 -0400</td>
      <td>GET</td>
      <td>/shuttle/countdown/liftoff.html</td>
      <td>HTTP/1.0</td>
      <td>304</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>199.120.110.21</td>
      <td>-</td>
      <td>-</td>
      <td>01/Jul/1995:00:00:11 -0400</td>
      <td>GET</td>
      <td>/shuttle/missions/sts-73/sts-73-patch-small.gif</td>
      <td>HTTP/1.0</td>
      <td>200</td>
      <td>4179</td>
      <td>4179</td>
    </tr>
  </tbody>
</table>
</div>




```python
dfCleanTyped.createOrReplaceTempView("cleantypedlog")
spark.sql("""
select endpoint, content_size
from cleantypedlog 
order by content_size_bytes desc
""").limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>endpoint</th>
      <th>content_size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>/shuttle/countdown/video/livevideo.jpeg</td>
      <td>6823936</td>
    </tr>
    <tr>
      <th>1</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>2</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>3</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>4</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>5</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>6</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>7</th>
      <td>/statistics/1995/bkup/Mar95_full.html</td>
      <td>3155499</td>
    </tr>
    <tr>
      <th>8</th>
      <td>/statistics/1995/Jun/Jun95_reverse_domains.html</td>
      <td>2973350</td>
    </tr>
    <tr>
      <th>9</th>
      <td>/statistics/1995/Jun/Jun95_reverse_domains.html</td>
      <td>2973350</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Left for you, clean the date column :)
# 1- Create a udf that parses that weird format,
# 2- Create a new column with a data tiem string that spark would understand
# 3- Add a new date-time column properly typed
# 4- Print your schema
```


```python

```


```python

```
