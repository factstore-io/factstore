export interface Store {
  id: string;
  name: string;
  created: string;
  factCount: number;
}

export interface Tag {
  k: string;
  v: string;
}

export interface Fact {
  id: string;
  appendedAt: string;
  factType: string;
  subject: string;
  tags: Tag[];
  metadata: Tag[];
  payload: string;
}

export interface StreamEntry {
  id: string;
  ts: string;
  type: string;
  subject: string;
  factId: string;
  isNew: boolean;
}
