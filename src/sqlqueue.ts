import { Queue } from 'dpwm-queue';
import type { Database, Statement } from 'better-sqlite3';

const prepare = (db: Database, x: object): unknown => Object.fromEntries(Object.entries(x).map(([k,v]) => [k, db.prepare(v)]));
const insertMany = <T>(db: Database, stmt: Statement) => db.transaction((xs) => xs.forEach((x: T) => stmt.run(x)))


export function prepareDB(db: Database): PreparedDB {
  // It is assumed that the DB will be already setup (eg WAL and synchronous=NORMAL)
  // But we will create the task table
  db.prepare(`CREATE TABLE IF NOT EXISTS task ( eventType INT, data JSON, created INTEGER, taken INTEGER, completed INTEGER, result JSON) `).run();

  const statements = prepare(db, {
    insert: "INSERT INTO task (eventType, data, created) VALUES (@eventType, @data, @created)",
    byType: "SELECT rowid, eventType, data from task WHERE eventType = @eventType AND completed IS NULL",
    taken: "UPDATE task SET taken = @taken where rowid = @rowid",
    completed: "UPDATE task SET completed = @completed, result = @result where rowid = @rowid",
    delete: "DELETE from task WHERE eventType = @eventType",
  }) as PreparedStatements;

  return {statements, db};
}

interface PreparedStatements {
  insert: Statement<{eventType: number, data: string, created: number}>;
  byType: Statement<{eventType: number}>;
  taken: Statement<{rowid: number, taken: number}>;
  completed: Statement<{rowid: number, completed: number, result: any}>;
  delete: Statement<{eventType: number}>;
}

interface PreparedDB {
  statements: PreparedStatements,
  db: Database,
}

export interface SQLQueueOptions {
  workers: number;
}

// Explicit is better than implicit.
type ListenCallback<T> = (data: T, rowid: number, eventType: number) => void;

type RowID = number;
type EventType = number;

interface Task<T> {
  data: T,
  rowid: RowID,
  eventType: EventType,
}

type SQLTask = Task<string>

namespace SQLTask {
  export function decode<T>({data, rowid, eventType} : SQLTask): Task<T> {
    return {data: JSON.parse(data) as T, rowid, eventType}
  }

  export function encode<T>({data, rowid, eventType} : Task<T>): SQLTask {
    return {data: JSON.stringify(data), rowid, eventType}
  }
}

export class SQLQueue<T> {
  queue: Queue<Task<T>>;
  callback: any;
  eventType: number;
  taskStatements: PreparedStatements;
  db: Database;
  _workers: number = 1;

  constructor(eventType: number, {statements, db}: PreparedDB) {
    const rawTasks = statements.byType.all({eventType}) as SQLTask[] ;
    const tasks = rawTasks.map(SQLTask.decode<T>);
    this.queue = new Queue<Task<T>>(tasks);
    this.eventType = eventType;
    this.taskStatements = statements;
    this.db = db;

    function listen(callback: (task: Task<T>) => void) {
      db.function(`task_callback_${eventType}`, (rowid, data) => {
        callback(SQLTask.decode({data: data as string, rowid: rowid as number, eventType: eventType as number}));
      })
      db.prepare(`CREATE TEMP TRIGGER task_listener_${eventType} AFTER INSERT ON main.task WHEN NEW.eventType = ${eventType} BEGIN select task_callback_${eventType}(NEW.rowid, NEW.data); END`).run()
    }

    listen(x => this.queue.push(x));
  }

  insertMany(xs: T[]): void {
    return insertMany(this.db, this.taskStatements.insert)(xs.map((data) => ({
      data: JSON.stringify(data),
      eventType: this.eventType,
      created: Date.now()})));
  }

  workers(n: number): SQLQueue<T> {
    this._workers = n;
    return this;
  }

  pForEach(f: (task: Task<T>) => Promise<any>, onError: any = () => {}): Promise<void> {
    const { taskStatements } = this;

    async function callback(task: Task<T>): Promise<void> {
      // log taken
      const {rowid} = task;
      taskStatements.taken.run({rowid, taken: Date.now()})
      const output: any = await f(task);
      const result = (output !== undefined) ? JSON.stringify(output) : undefined;
      taskStatements.completed.run({rowid, result, completed: Date.now()})
    }
    return this.queue.pForEach(callback, this._workers, 1, onError);
  }
}

/*
   export function SQLQueue(db: Database) {
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
