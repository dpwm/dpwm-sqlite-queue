import { test, expect } from 'vitest';
import { prepareDB, SQLQueue } from './sqlqueue.js';
import Database from 'better-sqlite3';


test("createQueue", () => {
	const db = prepareDB(new Database(":memory:"));
	const sq = new SQLQueue<number>(0, db);
});

test("insert into queue", () => {
	const db = prepareDB(new Database(":memory:"));
	const sq = new SQLQueue<number>(0, db);

	// Check we can insert
	sq.insertMany([4, 3, 2, 1]);
	expect(sq.queue.items.map(({data}) => data)).toStrictEqual([4, 3, 2, 1]);
});

test("iterate over queue", async () => {
	const db = prepareDB(new Database(":memory:"));
	const sq = new SQLQueue<number>(0, db);

	// Check we can insert
	let total = 0;
	sq.insertMany([4, 3, 2, 1]);
	sq.queue.close();
	await sq.pForEach(async ({data}) => {total += data; return 2 * data});

	expect(total).toBe(10);


});
