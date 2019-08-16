# drag and drop model pipeline
As a data engineer, I always need to deal with data pipeline or model pipeline from one end to another. It always require
different external storage in between pipelines. Sometimes csv is required, sometimes json is required and even database
is required. However, what we really care about is just the result of a single pipeline. Therefore, a higher level of
abstraction is required for data flowing.


# TODO
1. Dynamically import
2. Apache arrow: Essentially some sort of cross language data structure is required for sharing during one pipeline.

# Requirements
1. `mysql backend`: docker run -p 3306:3306 -v "$PWD/data":/var/lib/mysql --user 1000:1000 --name mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_USER=test -e MYSQL_PASSWORD=test -e MYSQL_DATABASE=data_pipeline -d mysql