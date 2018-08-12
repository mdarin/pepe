//
// Выгрузка данных из csv в БД SQL-запросами
//
// Решение предлагаюется построить на основе потоков Stream и интефейса для их конвееризации pipe
// -----------------------------------------
// If the data to be written can be generated or fetched on demand, 
// it is recommended to encapsulate the logic into a Readable and use stream.pipe().
//
// Streams official documentation
// [https://nodejs.org/dist/latest-v8.x/docs/api/stream.html#stream_writable_write_chunk_encoding_callback]
// -----------------------------------------
// то же верно и для Writable :)
//
'use strict';

//
//  Устроим собственный поток для записи
//
const { Writable } = require('stream');
class Writer extends Writable {
	constructor(opt = {}) {
		super(opt);

		console.log(' =[create DB connection]=');
		console.log(' BEGIN;');
		this.on('drain', () => {})
			.on('error', (err) => {})
			.on('close', () => {})
			.on('end', () => {})
			.on('finish', () => {
				console.log(' COMMIT;');
				console.log(' =[close DB connecction]=');
			});
		}

	_write(chunk, encoding, done) {
		// здеьс производится отправка запроса в БД по установленному соединению
		console.log('  ',chunk.toString());
		done();
		// вот так вот случается ошибка
		//done('error',chunk);
	}

}//eof class

//
//TODO(darin-m): здесь можно всё оборудовать для формирования запроса
//
class QueryValues {
	constructor(chunk) {
		//...
	}
}//eof class


//
// Устроим поток реализующий алгоритм преобразования
//
const {Transform} = require('stream');
class Transformer extends Transform {
	constructor(opt = {}) {
		super(opt);

		// запомнить поля таблцы
		this.tableHead = '';

		this.on('close', () => {})
			.on('drain', () => {})
			.on('error', (err) => {})
			.on('finish', () => {})
			.on('end', () => {})
			.on('pipe', () => {})
			.on('data', (chunk) => {});
	}

	_transform(chunk, encoding, done) {

		// исключительно для иллюстрации работы системы конвееров
		let splitted = chunk.split(';');
		let joined = '';
		let rx = /^([A-Za-z_-]+[;,\t]?)+$/;
		if (chunk.match(rx) != null) {
			this.tableHead = chunk.split(';');
			joined = '';	
		} else {
			if (splitted.length == this.tableHead.length) {
				joined = "insert into t (".toUpperCase() + this.tableHead.join(',') + ")\n  values (".toUpperCase() + splitted.map(function (v) {
					switch (v) {
					case '': return 'EMPTY';
					default: return v;
					}
				}).join(',') + ');\n';
			} else {
				// SKIP	
			}
		}

		done(null,joined);
   
	}

  _flush(done) {
		done();
	}

}// eof class



// готовим потоки

const fs = require('fs');

// поток чтения файла
// для исллюстрации малая часть файла
//const RR = fs.createReadStream('./test_case10.csv');
const RR = fs.createReadStream('./test_case.csv');

// поток записи в файл
const WR = fs.createWriteStream('./out.txt');

const Readline = require('readline');

// поток записи в БД с настройками по умолчанию
let w_opts = {};
const W = new Writer(w_opts);

let t_opts = {
		objectMode: true
   , readableObjectMode: false 
   , writableObjectMode: false
   , decodeStrings: true 
};
const T = new Transformer(t_opts);

// реализуем схему конвеера: поток из файла => построчное чтение => конвертер => поток записи в БД

// возможная схема исполнения запроса
// для запроса создаём поток записи который на создание открывает соедиение и начинает транзакцию,
// собирает построчно запрос при поступлении данных
// по событию finish даёт команду commit и исполняет транзакцию как единую конструкцию и разрушает соединение
// данная схема выбрана по причине сиюминутных желаний ввиду отсутвия общих требований к решению и не более того

console.log('Converting...');

// огранизуем построчное считывание для конвертирования
// интерфейсим поток файла с потоком конвертера
const rl = Readline.createInterface({
	input: RR,
	output: T 
});

rl.on('line', (input) => {
	switch (input) {
	case undefined: 
	case '':
	case '\n':
		break;
	default:
	// переложить данные построчно из cvs в конвертер
		T.write(input);
		T.write('\n');
		break;
	}
});
rl.on('close', () => {
	// закончить обработку
	T.end();
});
rl.on('error', (err) => {
	console.log('RL: error: ' + err);
});

// организуем вывод преобразованных в запросы данных 
// направить на запись в БД
T.pipe(W);
// наприаить в файл(имитация нагрузки)
T.pipe(WR);



















