import { Queue } from 'dpwm-queue';
import type { Database, Statement } from 'better-sqlite3';

const prepare = (db: Database, x: object) => Object.fromEntries(Object.entries(x).map(([k,v]) => [k, db.prepare(v)]));
const insertMany = <T>(db: Database, stmt: Statement) => db.transaction((xs) => xs.forEach((x: T) => stmt.run(x)))


// effectively a queue factory
export class SQLQueueWrapper {
  constructor(db: Database) {
    // It is assumed that the DB will be already setup (eg WAL and synchronous=NORMAL)
    // But we will create the task table
    db.prepare(`CREATE TABLE IF NOT EXISTS task ( event_type INT, data JSON, created INTEGER, taken INTEGER, completed INTEGER, result JSON) `).run();

    const task = prepare(db, {
      insert: "INSERT INTO task (event_type, data, created) VALUES (@event_type, @data, @created)",
      byType: "SELECT rowid, event_type, data from task WHERE event_type = @event_type AND completed IS NULL",
      taken: "UPDATE task SET taken = @taken where rowid = @rowid",
      completed: "UPDATE task SET completed = @completed, result = @result where rowid = @rowid",
      delete: "DELETE from task WHERE event_type = @event_type",
    });
  }
}

interface PreparedStatements {
  insert: Statement;
  byType: Statement;
  taken: Statement;
  completed: Statement;
  delete: Statement;
}

export interface SQLQueueOptions {
  workers: number;
}

class SQLQueue<T> {
  queue: Queue<T>;
  constructor(eventType: number, callback: (value: T) => Promise<void>, options: SQLQueueOptions, task: PreparedStatements) {
    this.queue = new Queue<T>(task.byType.all() as T[]);
  }
}

/*
export function SQLQueue(db: Database) {
  // It is assumed that the DB will be already setup (eg WAL and synchronous=NORMAL)
  // But we will create the task table
  db.prepare(`CREATE TABLE IF NOT EXISTS task ( event_type INT, data JSON, created INTEGER, taken INTEGER, completed INTEGER, result JSON) `).run();

  const task = prepare(db, {
    insert: "INSERT INTO task (event_type, data, created) VALUES (@event_type, @data, @created)",
    byType: "SELECT rowid, event_type, data from task WHERE event_type = @event_type AND completed IS NULL",
    taken: "UPDATE task SET taken = @taken where rowid = @rowid",
    completed: "UPDATE task SET completed = @completed, result = @result where rowid = @rowid",
    delete: "DELETE from task WHERE event_type = @event_type",
  });

  const registered = [];

  const Queue = (event_type, callback, options={workers:1}) => {
    const q = queue();

    function listen(callback) {
      db.function(`task_callback_${event_type}`, (rowid, data) => {
        //console.log("trigger");
        callback({...JSON.parse(data), _rowid: rowid, _event_type: event_type})});
      db.prepare(`CREATE TEMP TRIGGER task_listener_${event_type} AFTER INSERT ON main.task WHEN NEW.event_type = ${event_type} BEGIN select task_callback_${event_type}(NEW.rowid, NEW.data); END`).run()
    }

    task.byType.all({event_type})
      .forEach(({rowid, event_type, data}) => q.push({...JSON.parse(data), _rowid: rowid, _event_type: event_type}));
    listen((x) => q.push(x));

    return {
      q,

      insertMany(xs) {
        return insertMany(db, task.insert)(xs.map((data) => ({
          data: JSON.stringify(data),
          event_type,
          created: Date.now()})));
      },

      taken(rowid) {
        task.taken.run({rowid, taken: Date.now()})
      },

      completed(rowid, result) {
        task.completed.run({rowid, completed: Date.now(), result: JSON.stringify(result)})
      },

      all() {
        return select.taskByType.all({rowid}).map((x) => ({...x, data: JSON.parse(x.data)}));
      },

      join() {
        return q.pForEach(callback, options.workers);
      },

      toQueue() {
        return queue(select.taskByType.all({rowid}))
      },

      timings() {
        return select.taskStats.all({event_type});
      },

      cleanup() {
        return task.delete.run({event_type});;
      },

    }
  }

  

  return {
    task(callback, options) {
      const q = Queue(registered.length, callback, options);
      registered.push(q);
      return q;
    },
    async join() {
      await Promise.all(registered.map((x) => x.join()));
    }
  }
}
*/
