import Zongji from 'zongji';

export enum STATEMENTS {
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE',
  ALL = 'ALL'
}

export enum ACTIONS {
  writerows = STATEMENTS.INSERT,
  updaterows = STATEMENTS.UPDATE,
  deleterows = STATEMENTS.DELETE
}

export type IStatment = STATEMENTS.ALL | STATEMENTS.DELETE | STATEMENTS.INSERT | STATEMENTS.UPDATE;
export type IDataType<T> = null | T;
export type IHandler = (event: IEvent) => void;

export interface IMysqlOptions {
  host: string;
  user: string;
  password: string;
  port?: number | string;
}

export interface ITrigger {
  handler: IHandler;
  tag?: string;
  expression?: string;
  statement?: IStatment;
  enable?: boolean;
}

export interface IEvent {
  action: string;
  timestamp: Date;
  database: string;
  table: string;
  changedColumns: string[];
  data: { old: IDataType<object>; new: IDataType<object> };
}

export interface ITemplate {
  path: string;
  timestamp: Date;
}

export default class MysqlEvent {
  private readonly zongji: Zongji;
  private triggers: Map<string, ITrigger> = new Map();
  private tags: Map<string, string> = new Map();

  constructor(mysqlSettings: IMysqlOptions) {
    this.zongji = new Zongji(mysqlSettings);
    this.init();
  }

  public static get STATEMENTS() {
    return STATEMENTS;
  }

  /**
   * Adds new trigger if tag or expressions this one is unique
   * If tag or expressions exists then returns null
   * @param {ITrigger} trigger
   * @returns {string|null}
   */
  public add(trigger: ITrigger): string | null {
    if (!trigger) {
      return;
    }

    if (typeof trigger.handler !== 'function') {
      throw new Error('Handler must be a function!');
    }

    trigger.enable = true;
    trigger.statement = trigger.statement || STATEMENTS.ALL;
    trigger.tag = trigger.tag || this.getRandomTag();
    trigger.expression = this.checkExpression(trigger.expression);

    const { expression, tag } = trigger;
    if (this.triggers.has(tag) || this.tags.has(expression)) {
      return null;
    }

    this.tags.set(expression, tag);
    this.triggers.set(tag, trigger);

    return tag;
  }

  /**
   * Parses the expression and returns valid this one
   * @param {string} expression
   * @returns {string}
   */
  public checkExpression(expression: string): string {
    if (!expression || expression === '*' || expression === '*.*') {
      return '*';
    }

    const parts = expression.split('.');
    if (parts.length !== 2) {
      throw new Error('The expression must consist of two parts [ database/*.table/* ] or character [ */*.* ]');
    }

    return parts[0] === '' ? `*.${parts[1]}` : parts[1] === '' ? `${parts[0]}.*` : expression;
  }

  public stop(tag: string): MysqlEvent {
    const trigger = this.triggers.get(tag);
    trigger && (trigger.enable = false);
    return this;
  }

  public start(tag: string): MysqlEvent {
    const trigger = this.triggers.get(tag);
    trigger && (trigger.enable = true);
    return this;
  }

  public remove(tag: string): MysqlEvent {
    this.triggers.delete(tag);
    return this;
  }

  public stopAll(): MysqlEvent {
    this.zongji.stop();
    return this;
  }

  public satrtAll(): MysqlEvent {
    this.init();
    return this;
  }

  /**
   * Main event handler
   * @param {any} blEvent
   * @param {string} action
   */
  private eventHandler(blEvent: any, action: string): void {
    const { tableMap, tableId, timestamp, rows } = blEvent;

    const path = `${tableMap[tableId].parentSchema}.${tableMap[tableId].tableName}`;
    const triggers = this.getTriggers(path, action);
    if (!triggers.length) {
      return;
    }

    const events = this.getManyEvents(action, rows, { path, timestamp });
    for (const trigger of triggers) {
      events.forEach((evt: IEvent) => trigger.handler(evt));
    }
  }

  /**
   * Returns many events for client
   * @param {IEvent} event
   * @param {string} action
   * @param {any[]} rows
   * @returns {IEvent[]}
   */
  private getManyEvents(action: string, rows: any[], template: ITemplate): IEvent[] {
    const payloads: IEvent[] = [];

    for (const row of rows) {
      const event = this.getEventTemplate(action, template);
      if (action === STATEMENTS.INSERT) {
        event.data.new = row;
      }

      if (action === STATEMENTS.DELETE) {
        event.data.old = row;
      }

      if (action === STATEMENTS.UPDATE) {
        const changedColumns: Set<string> = new Set();
        for (const key in row.before) {
          if (String(row.before[key]) !== String(row.after[key])) {
            changedColumns.add(key);
          }
        }

        event.data.old = row.before;
        event.data.new = row.after;
        event.changedColumns = Array.from(changedColumns);
      }

      payloads.push(event);
    }

    return payloads;
  }

  /**
   * Returns event template
   * @param {string} action
   * @param {template} ITemplate
   * @returns {IEvent}
   */
  private getEventTemplate(action: string, template: ITemplate): IEvent {
    const [database, table] = template.path.split('.');

    return {
      action,
      database,
      table,
      timestamp: template.timestamp,
      changedColumns: [],
      data: { old: null, new: null }
    };
  }

  /**
   * Returns array active triggers by path and actions
   * @param {string} path
   * @param {string} action
   * @returns {ITrigger[]}
   */
  private getTriggers(path: string, action: string): ITrigger[] {
    const [db, table] = path.split('.');

    const triggers: ITrigger[] = [];
    const pathOptions = ['*', `${db}.*`, `*.${table}`, `${db}.${table}`];

    pathOptions.forEach((exp: string) => {
      const tag = this.tags.get(exp);
      if (!tag) {
        return;
      }

      const trigger = this.triggers.get(tag);
      const { statement, enable } = trigger;
      if ((statement === STATEMENTS.ALL || statement === action) && enable === true) {
        triggers.push(trigger);
      }
    });

    return triggers;
  }

  private getRandomTag(): string {
    return Math.random()
      .toString(36)
      .replace(/[^a-z|0-9]+/g, '')
      .substr(0, 7);
  }

  private binlogCallback(event: any) {
    const action = ACTIONS[event.getEventName()];
    if (!action) {
      return;
    }

    this.eventHandler(event, action);
  }

  private init(): void {
    this.zongji.removeListener('binlog', this.binlogCallback.bind(this));
    this.zongji.on('binlog', this.binlogCallback.bind(this));

    this.zongji.on('error', (err: Error) => {
      this.zongji.stop();
      this.zongji.removeListener('binlog', this.binlogCallback.bind(this));
      console.error(err);
    });

    process.on('SIGINT', () => {
      this.zongji.stop();
      process.exit();
    });

    this.zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      startAtEnd: true
    });
  }
}
