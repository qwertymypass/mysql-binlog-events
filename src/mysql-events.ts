import Zongji from 'zongji';

export enum STATEMENTS {
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELEETE',
  ALL = 'ALL'
}

export enum ACTIONS {
  writerows = STATEMENTS.INSERT,
  updaterows = STATEMENTS.UPDATE,
  deleterows = STATEMENTS.DELETE
}

export type IStatment = STATEMENTS.ALL | STATEMENTS.DELETE | STATEMENTS.INSERT | STATEMENTS.UPDATE;
export type IDataType = null | object;
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
  timestamp: number;
  database: string;
  table: string;
  changedColumns: string[];
  data: { old: IDataType; new: IDataType };
}

export default class MysqlEvent {
  private readonly zongji: Zongji;
  private triggers: Map<string, ITrigger>;
  private tags: Map<string, string>;

  constructor(mysqlSettings: IMysqlOptions) {
    this.zongji = new Zongji(mysqlSettings);
    this.triggers = new Map();
    this.tags = new Map();

    this.init();
  }

  public static get STATEMENTS() {
    return STATEMENTS;
  }

  /**
   * Adds new trigger if tag or expressions this one is unique
   * If tag or expressions exists then returns false
   * @param {ITrigger} trg
   * @returns {string|boolean}
   */
  public add(trg: ITrigger): string | boolean {
    if (!trg) {
      return;
    }

    if (typeof trg.handler !== 'function') {
      throw new Error('Handler must be a function!');
    }

    trg.enable = true;
    trg.statement = trg.statement || STATEMENTS.ALL;
    trg.tag = trg.tag || this.getRandomTag();
    trg.expression = this.checkExpression(trg.expression);

    const { expression, tag } = trg;
    if (this.triggers.has(tag) || this.tags.has(expression)) {
      return false;
    }

    this.tags.set(expression, tag);
    this.triggers.set(tag, trg);

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

    if (expression.split('.').length !== 2) {
      throw new Error('The expression must consist of two parts [ database/*.table/* ] or character [ */*.* ]');
    }

    return expression;
  }

  /**
   * Stop trigger by tag if exists (enabled = false)
   * @param {string} tag
   * @returns {MysqlEvent}
   */
  public stop(tag: string): MysqlEvent {
    const trg = this.triggers.get(tag);
    trg && (trg.enable = false);
    return this;
  }

  /**
   * Starts trigger by tag if exists (enabled = true)
   * @param {string} tag
   * @returns {MysqlEvent}
   */
  public start(tag: string): MysqlEvent {
    const trg = this.triggers.get(tag);
    trg && (trg.enable = true);
    return this;
  }

  /**
   * Removes tigger by tag if exists
   * @param {string} tag
   * @returns {MysqlEvent}
   */
  public remove(tag: string): MysqlEvent {
    this.triggers.delete(tag);
    return this;
  }

  /**
   * Stop listening binlog
   * @returns {MysqlEvent}
   */
  public stopAll(): MysqlEvent {
    this.zongji.stop();
    return this;
  }

  /**
   * Starts listening binlog
   * @returns {MysqlEvent}
   */
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

    const eventObject = this.getEventObject(action, path, timestamp);
    const events = this.getManyEvents(eventObject, action, rows);
    for (const trg of triggers) {
      events.forEach((evt: IEvent) => trg.handler(evt));
    }
  }

  /**
   * Parse blEvent for make evet object
   * @param {string} action
   * @param {string} path
   * @param {number} timestamp
   * @returns {IEvent}
   */
  private getEventObject(action: string, path: string, timestamp: number): IEvent {
    const [database, table] = path.split('.');

    return {
      action,
      database,
      table,
      timestamp,
      changedColumns: [],
      data: { old: null, new: null }
    };
  }

  /**
   * Returns many events for client
   * @param {IEvent} event
   * @param {string} action
   * @param {any[]} rows
   * @returns {IEvent[]}
   */
  private getManyEvents(event: IEvent, action: string, rows: any[]): IEvent[] {
    const payloads: IEvent[] = [];
    rows.forEach((row: any) => {
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
    });

    return payloads;
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

      const trg = this.triggers.get(tag);
      const { statement, enable } = trg;
      if ((statement === STATEMENTS.ALL || statement === action) && enable === true) {
        triggers.push(trg);
      }
    });

    return triggers;
  }

  /**
   * Generates random string for tag
   * @returns {string}
   */
  private getRandomTag(): string {
    return Math.random()
      .toString(36)
      .replace(/[^a-z|0-9]+/g, '')
      .substr(0, 7);
  }

  /**
   * Initialize zongji. Adds handlers
   */
  private init(): void {
    const onBinlog = ((blEvent: any) => {
      const action = ACTIONS[blEvent.getEventName()];
      if (!action) {
        return;
      }

      this.eventHandler(blEvent, action);
    }).bind(this);

    this.zongji.on('binlog', onBinlog);

    this.zongji.on('error', (err: Error) => {
      this.zongji.stop();
      this.zongji.removeListener('binlog', onBinlog);
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
