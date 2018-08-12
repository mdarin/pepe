# pepe 

### Задача

Необходимо написать импортер данных из файла test_case.csv в базу данных PostgreSQL. 
Структуру можно использовать такую же как в файле.

### Схема решения

из CSV File -> преобразовать в SQLQuery формат -> сохранить в DataBase
Readable.pipe(Transform).pipe(Writable);


### Выгрузка данных из csv в БД SQL-запросами

Решение предлагаюется построить на основе потоков и интефейса для их конвееризации pipe

If the data to be written can be generated or fetched on demand, 
it is recommended to encapsulate the logic into a Readable and use stream.pipe().

Streams official documentation
[https://nodejs.org/dist/latest-v8.x/docs/api/stream.html#stream_writable_write_chunk_encoding_callback]

То же верно и для Writable :)


## Запуск

$nodejs csv2db.js

предполагяется что файл test_case.csv лежит рядом со скриптом

к потоку преобразования подключено два потока вывода

1.поток записи в БД

2.поток записи в текстовый файл
