export interface Displayable {
  text: string;
  render(): string;
}

export class Greeter {
  message: string;

  constructor(message: string) {
    this.message = message;
  }

  greet(name: string): string {
    return `${this.message}, ${name}`;
  }
}

export function helper(): string {
  return "hello";
}
