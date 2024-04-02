import { test, expect } from 'vitest';
import { prepareDB, SQLQueue } from './sqlqueue.js';
import Database from 'better-sqlite3';

test("prepareDB", () => {
	const db = new Database(":memory:");
	const statements = prepareDB(db);
});

test("createQueue", () => {
	const db = new Database(":memory:");
	const statements = prepareDB(db);
	const sq = new SQLQueue(0, statements, db);
});

test("insert into queue", () => {
	const db = new Database(":memory:");
	const statements = prepareDB(db);
	const sq = new SQLQueue<number>(0, statements, db);

	// Check we can insert
	sq.insertMany([4, 3, 2, 1]);
	expect(sq.queue.items.map(({data}) => data)).toStrictEqual([4, 3, 2, 1]);
});
