export enum STATEMENTS {
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELEETE',
  ALL = 'ALL'
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
