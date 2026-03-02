import { EventEmitter } from 'events';

class Greeter extends EventEmitter {
  constructor(name: string) {
    super();
  }

  greet(): string {
    return 'hello';
  }
}

function formatGreeting(msg: string): string {
  return msg.toUpperCase();
}
